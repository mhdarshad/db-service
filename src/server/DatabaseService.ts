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

    // 🟢 Read/Fetch a single record by multiple conditions
    abstract get(conditions: Record<string, any>): Promise<any>;

    // 🟢 Read multiple records with filtering, sorting, and pagination
    abstract getAll(
        conditions?: Record<string, any>,
        sort?: Record<string, "ASC" | "DESC">,
        limit?: number,
        offset?: number
    ): Promise<any[]>;

    // 🟢 Update a record based on multiple conditions
    abstract update(data: Record<string, any>, conditions: Record<string, any>): Promise<any>;

    // 🟢 Increment/Decrement a field based on multiple conditions
    abstract increment(field: string, amount: number, conditions: Record<string, any>): Promise<any>;

    // 🟢 Delete a record based on multiple conditions
    abstract delete(conditions: Record<string, any>): Promise<any>;

    // 🟢 Soft Delete (Mark a record as deleted instead of removing it)
    abstract softDelete(conditions: Record<string, any>, deletedField?: string): Promise<any>;

    // 🟢 Count records that match multiple conditions
    abstract count(conditions: Record<string, any>): Promise<number>;

    // 🟢 Check if a record exists based on multiple conditions
    abstract exists(conditions: Record<string, any>): Promise<boolean>;

    // 🟢 Find records using advanced search (LIKE, FULL TEXT SEARCH, etc.)
    abstract search(query: string, fields: string[]): Promise<any[]>;

    // 🟢 Transaction Handling (For MySQL, DynamoDB Transactions)
    abstract transaction(actions: any[]): Promise<any>;
}

export default DatabaseService;
