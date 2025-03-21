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

    async create(data: Record<string, any>): Promise<number> {
        const fields = Object.keys(data).join(", ");
        const values = Object.values(data).map(value => 
            typeof value === "object" ? JSON.stringify(value) : value
        ); // Convert objects to JSON strings
        const placeholders = values.map(() => "?").join(", ");
        const query = `INSERT INTO ${this.tableName} (${fields}) VALUES (${placeholders})`;
    
        const [result] = await this.pool.execute<ResultSetHeader>(query, values);
        return result.insertId;
    }

    async bulkInsert(dataArray: Record<string, any>[]): Promise<number> {
        if (!dataArray.length) return 0;

        const fields = Object.keys(dataArray[0]).join(", ");
        const values = dataArray.map(Object.values);
        const placeholders = dataArray
            .map(() => `(${Object.values(dataArray[0]).map(() => "?").join(", ")})`)
            .join(", ");
        const query = `INSERT INTO ${this.tableName} (${fields}) VALUES ${placeholders}`;

        const [result] = await this.pool.query<ResultSetHeader>(query, values.flat());
        return result.affectedRows;
    }

    async get(condition: Record<string, any>): Promise<RowDataPacket | null> {
        const field = Object.keys(condition)[0];
        const value = Object.values(condition)[0];
        const query = `SELECT * FROM ${this.tableName} WHERE ${field} = ? LIMIT 1`;

        const [rows] = await this.pool.execute<RowDataPacket[]>(query, [value]);
        return rows.length ? rows[0] : null;
    }

    async getAll(
        conditions: Record<string, any> = {},
        sort: Record<string, "ASC" | "DESC"> = {},
        limit: number = 10,
        offset: number = 0
    ): Promise<RowDataPacket[]> {
        let query = `SELECT * FROM ${this.tableName}`;
        let values: any[] = [];

        if (Object.keys(conditions).length) {
            const whereClauses = Object.keys(conditions)
                .map((key) => `${key} = ?`)
                .join(" AND ");
            query += ` WHERE ${whereClauses}`;
            values = Object.values(conditions);
        }

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

    async update(data: Record<string, any>, condition: Record<string, any>): Promise<boolean> {
        const updateFields = Object.keys(data)
            .map((key) => `${key} = ?`)
            .join(", ");
        const conditionField = Object.keys(condition)[0];
        const conditionValue = Object.values(condition)[0];

        const query = `UPDATE ${this.tableName} SET ${updateFields} WHERE ${conditionField} = ?`;
        const values = [...Object.values(data), conditionValue];

        const [result] = await this.pool.execute<ResultSetHeader>(query, values);
        return result.affectedRows > 0;
    }

    async increment(field: string, amount: number, condition: Record<string, any>): Promise<boolean> {
        const conditionField = Object.keys(condition)[0];
        const conditionValue = Object.values(condition)[0];

        const query = `UPDATE ${this.tableName} SET ${field} = ${field} + ? WHERE ${conditionField} = ?`;
        const [result] = await this.pool.execute<ResultSetHeader>(query, [amount, conditionValue]);
        return result.affectedRows > 0;
    }

    async delete(condition: Record<string, any>): Promise<boolean> {
        const field = Object.keys(condition)[0];
        const value = Object.values(condition)[0];
        const query = `DELETE FROM ${this.tableName} WHERE ${field} = ?`;

        const [result] = await this.pool.execute<ResultSetHeader>(query, [value]);
        return result.affectedRows > 0;
    }

    async softDelete(condition: Record<string, any>, deletedField: string = "isDeleted"): Promise<boolean> {
        return this.update({ [deletedField]: true }, condition);
    }

    async count(condition: Record<string, any>= {}): Promise<number> {
         let values: any[] = [];
         const query = `SELECT COUNT(*) as count FROM ${this.tableName} `;
        if (Object.keys(conditions).length) {
            const whereClauses = Object.keys(conditions)
                .map((key) => `${key} = ?`)
                .join(" AND ");
            query += ` WHERE ${whereClauses}`;
            values = Object.values(conditions);
        }

        const [rows] = await this.pool.execute<RowDataPacket[]>(query, values);
        return rows[0].count;
    }

    async exists(condition: Record<string, any>): Promise<boolean> {
        return (await this.count(condition)) > 0;
    }

    async search(query: string, fields: string[]): Promise<RowDataPacket[]> {
        const searchCondition = fields.map((field) => `${field} LIKE ?`).join(" OR ");
        const values = fields.map(() => `%${query}%`);

        const sql = `SELECT * FROM ${this.tableName} WHERE ${searchCondition}`;
        const [rows] = await this.pool.execute<RowDataPacket[]>(sql, values);
        return rows;
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
                        await this.increment(Object.keys(data!)[0], Object.values(data!)[0], condition!);
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
