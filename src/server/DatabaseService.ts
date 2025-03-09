abstract class DatabaseService {
    protected tableName: string;

    constructor(tableName: string) {
        if (new.target === DatabaseService) {
            throw new Error("Cannot instantiate abstract class DatabaseService");
        }
        this.tableName = tableName;
    }

    // 🟢 Create a new record
    abstract create(data: Record<string, any>): Promise<any>;

    // 🟢 Bulk Insert
    abstract bulkInsert(dataArray: Record<string, any>[]): Promise<any>;

    // 🟢 Read/Fetch a single record by condition
    abstract get(condition: Record<string, any>): Promise<any>;

    // 🟢 Read multiple records with filtering, sorting, and pagination
    abstract getAll(
        conditions?: Record<string, any>,
        sort?: Record<string, any>,
        limit?: number,
        offset?: number
    ): Promise<any[]>;

    // 🟢 Update a record based on a condition
    abstract update(data: Record<string, any>, condition: Record<string, any>): Promise<any>;

    // 🟢 Increment/Decrement a field (e.g., increase wallet balance)
    abstract increment(field: string, amount: number, condition: Record<string, any>): Promise<any>;

    // 🟢 Delete a record based on condition
    abstract delete(condition: Record<string, any>): Promise<any>;

    // 🟢 Soft Delete (Mark a record as deleted instead of removing it)
    abstract softDelete(condition: Record<string, any>, deletedField?: string): Promise<any>;

    // 🟢 Count records that match a condition
    abstract count(condition: Record<string, any>): Promise<number>;

    // 🟢 Check if a record exists
    abstract exists(condition: Record<string, any>): Promise<boolean>;

    // 🟢 Find records using advanced search (LIKE, FULL TEXT SEARCH, etc.)
    abstract search(query: string, fields: string[]): Promise<any[]>;

    // 🟢 Transaction Handling (For MySQL, DynamoDB Transactions)
    abstract transaction(actions: any[]): Promise<any>;
}

export default DatabaseService;
