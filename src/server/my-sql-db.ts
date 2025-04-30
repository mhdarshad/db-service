import mysql, { Pool, PoolConnection, RowDataPacket, ResultSetHeader } from "mysql2/promise";
import DatabaseService from "./DatabaseService";

interface TransactionAction {
    method: "create" | "update" | "delete" | "increment";
    data?: Record<string, any>;
    condition?: Record<string, any>;
}

class MySQLService extends DatabaseService {
    private pool: Pool;
    private readonly maxRetries = 3;
    private readonly retryDelay = 1000; // ms

    constructor(tableName: string) {
        super(tableName);
        if (!process.env.DB_HOST || !process.env.DB_USER || !process.env.DB_PASSWORD || !process.env.DB_NAME) {
            throw new Error('Missing required database configuration');
        }
        this.pool = mysql.createPool({
            host: process.env.DB_HOST,
            user: process.env.DB_USER,
            password: process.env.DB_PASSWORD,
            database: process.env.DB_NAME,
            waitForConnections: true,
            connectionLimit: 10,
            queueLimit: 0,
            enableKeepAlive: true,
            keepAliveInitialDelay: 0,
        });
    }

    private validateData(data: Record<string, any>): void {
        if (!data || Object.keys(data).length === 0) {
            throw new Error('Data object cannot be empty');
        }
    }

    private async executeWithRetry<T>(operation: () => Promise<T>): Promise<T> {
        let lastError: Error | null = null;
        for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
            try {
                return await operation();
            } catch (error) {
                lastError = error as Error;
                if (attempt < this.maxRetries) {
                    await new Promise(resolve => setTimeout(resolve, this.retryDelay * attempt));
                }
            }
        }
        throw lastError;
    }

    async create(data: Record<string, any>): Promise<number> {
        this.validateData(data);
        return this.executeWithRetry(async () => {
            const fields = Object.keys(data).join(", ");
            const values = Object.values(data).map(value => 
                typeof value === "object" ? JSON.stringify(value) : value
            );
            const placeholders = values.map(() => "?").join(", ");
            const query = `INSERT INTO ${this.tableName} (${fields}) VALUES (${placeholders})`;
        
            const [result] = await this.pool.execute<ResultSetHeader>(query, values);
            return result.insertId;
        });
    }

    async bulkInsert(dataArray: Record<string, any>[]): Promise<number> {
        if (!Array.isArray(dataArray)) {
            throw new Error('Input must be an array');
        }
        if (dataArray.length === 0) return 0;

        return this.executeWithRetry(async () => {
            const firstItem = dataArray[0];
            this.validateData(firstItem);
            
            const fields = Object.keys(firstItem).join(", ");
            const values = dataArray.map(item => {
                if (Object.keys(item).length !== Object.keys(firstItem).length) {
                    throw new Error('All items in array must have the same structure');
                }
                return Object.values(item).map(value =>
                    typeof value === "object" ? JSON.stringify(value) : value
                );
            });
            
            const placeholders = dataArray
                .map(() => `(${Object.keys(firstItem).map(() => "?").join(", ")})`)
                .join(", ");
            const query = `INSERT INTO ${this.tableName} (${fields}) VALUES ${placeholders}`;

            const [result] = await this.pool.query<ResultSetHeader>(query, values.flat());
            return result.affectedRows;
        });
    }

    async get(condition: Record<string, any>): Promise<RowDataPacket | null> {
        this.validateData(condition);
        return this.executeWithRetry(async () => {
            const field = Object.keys(condition)[0];
            const value = Object.values(condition)[0];
            const query = `SELECT * FROM ${this.tableName} WHERE ${field} = ? LIMIT 1`;

            const [rows] = await this.pool.execute<RowDataPacket[]>(query, [value]);
           console.log(rows);
            return rows.length>0 ? rows[0] : null;
        });
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
        this.validateData(data);
        this.validateData(condition);
        
        return this.executeWithRetry(async () => {
            const updateFields = Object.keys(data)
                .map((key) => `${key} = ?`)
                .join(", ");
            const conditionField = Object.keys(condition)[0];
            const conditionValue = Object.values(condition)[0];

            const query = `UPDATE ${this.tableName} SET ${updateFields} WHERE ${conditionField} = ?`;
            const values = Object.values(data).map(value =>
                typeof value === "object" ? JSON.stringify(value) : value
            );
            values.push(conditionValue);

            const [result] = await this.pool.execute<ResultSetHeader>(query, values);
            return result.affectedRows > 0;
        });
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

    async count(conditions: Record<string, any>= {}): Promise<number> {
         let values: any[] = [];
         let query = `SELECT COUNT(*) as count FROM ${this.tableName} `;
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
        if (!actions || actions.length === 0) {
            throw new Error('Transaction actions cannot be empty');
        }

        let connection: PoolConnection | null = null;
        try {
            connection = await this.pool.getConnection();
            await connection.beginTransaction();

            for (const action of actions) {
                const { method, data, condition } = action;
                if (!method) {
                    throw new Error('Transaction action method is required');
                }

                switch (method) {
                    case "create":
                        if (!data) throw new Error('Data is required for create action');
                        await this.create(data);
                        break;
                    case "update":
                        if (!data || !condition) throw new Error('Data and condition are required for update action');
                        await this.update(data, condition);
                        break;
                    case "delete":
                        if (!condition) throw new Error('Condition is required for delete action');
                        await this.delete(condition);
                        break;
                    case "increment":
                        if (!data || !condition) throw new Error('Data and condition are required for increment action');
                        await this.increment(Object.keys(data)[0], Object.values(data)[0], condition);
                        break;
                    default:
                        throw new Error(`Invalid transaction method: ${method}`);
                }
            }

            await connection.commit();
            return true;
        } catch (error) {
            if (connection) {
                await connection.rollback();
            }
            throw error;
        } finally {
            if (connection) {
                connection.release();
            }
        }
    }
}

export default MySQLService;