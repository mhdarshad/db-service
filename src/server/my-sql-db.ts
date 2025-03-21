import mysql, { Pool, PoolConnection, RowDataPacket, ResultSetHeader } from "mysql2/promise";
import DatabaseService from "./DatabaseService";

interface TransactionAction {
    method: "create" | "update" | "delete" | "increment";
    data?: Record<string, any>;
    condition?: Record<string, any>;
}

class MySQLService extends DatabaseService {
    private pool: Pool;

    constructor(tableName: string) {
        super(tableName);
        this.pool = mysql.createPool({
            host: process.env.DB_HOST as string,
            user: process.env.DB_USER as string,
            password: process.env.DB_PASSWORD as string,
            database: process.env.DB_NAME as string,
            waitForConnections: true,
            connectionLimit: 10,
            queueLimit: 0,
        });
    }

    private buildWhereClause(conditions: Record<string, any>): { clause: string, values: any[] } {
        if (!Object.keys(conditions).length) return { clause: "", values: [] };
        const clause = Object.keys(conditions).map((key) => `${key} = ?`).join(" AND ");
        const values = Object.values(conditions);
        return { clause: `WHERE ${clause}`, values };
    }

    async create(data: Record<string, any>): Promise<number> {
        const fields = Object.keys(data).join(", ");
        const values = Object.values(data).map(value => 
            typeof value === "object" ? JSON.stringify(value) : value
        );
        const placeholders = values.map(() => "?").join(", ");
        const query = `INSERT INTO ${this.tableName} (${fields}) VALUES (${placeholders})`;
    
        const [result] = await this.pool.execute<ResultSetHeader>(query, values);
        return result.insertId;
    }

    async get(conditions: Record<string, any>): Promise<RowDataPacket | null> {
        const { clause, values } = this.buildWhereClause(conditions);
        const query = `SELECT * FROM ${this.tableName} ${clause} LIMIT 1`;
        const [rows] = await this.pool.execute<RowDataPacket[]>(query, values);
        return rows.length ? rows[0] : null;
    }

    async getAll(
        conditions: Record<string, any> = {},
        sort: Record<string, "ASC" | "DESC"> = {},
        limit: number = 10,
        offset: number = 0
    ): Promise<RowDataPacket[]> {
        const { clause, values } = this.buildWhereClause(conditions);
        let query = `SELECT * FROM ${this.tableName} ${clause}`;
    
        if (Object.keys(sort).length) {
            const orderBy = Object.entries(sort)
                .map(([key, direction]) => `${key} ${direction}`)
                .join(", ");
            query += ` ORDER BY ${orderBy}`;
        }
    
        query += ` LIMIT ? OFFSET ?`;
        values.push(limit, offset);
        
        const [rows] = await this.pool.execute<RowDataPacket[]>(query, values);
        return rows;
    }

    async update(data: Record<string, any>, conditions: Record<string, any>): Promise<boolean> {
        const updateFields = Object.keys(data).map((key) => `${key} = ?`).join(", ");
        const { clause, values: conditionValues } = this.buildWhereClause(conditions);
        const query = `UPDATE ${this.tableName} SET ${updateFields} ${clause}`;
        const values = [...Object.values(data), ...conditionValues];

        const [result] = await this.pool.execute<ResultSetHeader>(query, values);
        return result.affectedRows > 0;
    }

    async delete(conditions: Record<string, any>): Promise<boolean> {
        const { clause, values } = this.buildWhereClause(conditions);
        const query = `DELETE FROM ${this.tableName} ${clause}`;
        const [result] = await this.pool.execute<ResultSetHeader>(query, values);
        return result.affectedRows > 0;
    }

    async transaction(actions: TransactionAction[]): Promise<boolean> {
        const connection: PoolConnection = await this.pool.getConnection();
        await connection.beginTransaction();
        try {
            for (const action of actions) {
                const { method, data, condition } = action;
                switch (method) {
                    case "create":
                        await this.create(data!);
                        break;
                    case "update":
                        await this.update(data!, condition!);
                        break;
                    case "delete":
                        await this.delete(condition!);
                        break;
                    case "increment":
                        const field = Object.keys(data!)[0];
                        const amount = Object.values(data!)[0];
                        const { clause, values } = this.buildWhereClause(condition!);
                        const query = `UPDATE ${this.tableName} SET ${field} = ${field} + ? ${clause}`;
                        await connection.execute(query, [amount, ...values]);
                        break;
                    default:
                        throw new Error(`Invalid transaction method: ${method}`);
                }
            }
            await connection.commit();
            return true;
        } catch (error) {
            await connection.rollback();
            throw error;
        } finally {
            connection.release();
        }
    }
}

export default MySQLService;
