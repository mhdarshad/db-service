import { MongoClient, Db, Collection, ClientSession } from "mongodb";
import DatabaseService from "./DatabaseService";

let mongoClient: MongoClient | null = null; // Shared MongoDB client instance

interface TransactionAction {
    method: "create" | "update" | "delete" | "increment";
    data?: any;
    condition?: Record<string, any>;
}

class MongoDBService extends DatabaseService {
    private db: Db | null = null;
    private collection: Collection | null = null;

    constructor(collectionName: string) {
        super(collectionName);
    }

    private async connect(): Promise<void> {
        if (!mongoClient) {
            mongoClient = new MongoClient(process.env.MONGO_URI as string, {
                monitorCommands: true,
            });
            await mongoClient.connect();
        }
        this.db = mongoClient.db(process.env.MONGO_DB_NAME as string); // Fixed typo
        this.collection = this.db.collection(this.tableName);
    }

    async create(data: any): Promise<string> {
        await this.connect();
        const result = await this.collection!.insertOne(data);
        return result.insertedId.toString();
    }

    async bulkInsert(dataArray: any[]): Promise<string[]> {
        await this.connect();
        const result = await this.collection!.insertMany(dataArray);
        return Object.values(result.insertedIds).map(id => id.toString());
    }

    async get(condition: Record<string, any>): Promise<any | null> {
        await this.connect();
        return await this.collection!.findOne(condition);
    }

    async getAll(
        conditions: Record<string, any> = {},
        sort: Record<string, any> = {},
        limit: number = 10,
        offset: number = 0
    ): Promise<any[]> {
        await this.connect();
        return await this.collection!.find(conditions).sort(sort).skip(offset).limit(limit).toArray();
    }

    async update(data: any, condition: Record<string, any>): Promise<boolean> {
        await this.connect();
        const result = await this.collection!.updateOne(condition, { $set: data });
        return result.modifiedCount > 0;
    }

    async increment(field: string, amount: number, condition: Record<string, any>): Promise<boolean> {
        await this.connect();
        const result = await this.collection!.updateOne(condition, { $inc: { [field]: amount } });
        return result.modifiedCount > 0;
    }

    async delete(condition: Record<string, any>): Promise<boolean> {
        await this.connect();
        const result = await this.collection!.deleteOne(condition);
        return result.deletedCount > 0;
    }

    async softDelete(condition: Record<string, any>, deletedField: string = "isDeleted"): Promise<boolean> {
        await this.connect();
        const result = await this.collection!.updateOne(condition, { $set: { [deletedField]: true } });
        return result.modifiedCount > 0;
    }

    async count(condition: Record<string, any>): Promise<number> {
        await this.connect();
        return await this.collection!.countDocuments(condition);
    }

    async exists(condition: Record<string, any>): Promise<boolean> {
        await this.connect();
        const record = await this.collection!.findOne(condition, { projection: { _id: 1 } });
        return record !== null;
    }

    async search(query: string, fields: string[]): Promise<any[]> {
        await this.connect();
        const searchConditions = {
            $or: fields.map((field) => ({
                [field]: { $regex: query, $options: "i" },
            })),
        };
        return await this.collection!.find(searchConditions).toArray();
    }

    async transaction(actions: TransactionAction[]): Promise<boolean> {
        await this.connect();
        const session: ClientSession = mongoClient!.startSession();
        let success = false;

        try {
            await session.withTransaction(async () => {
                for (const action of actions) {
                    const { method, data, condition } = action;
                    switch (method) {
                        case "create":
                            await this.collection!.insertOne(data!, { session });
                            break;
                        case "update":
                            await this.collection!.updateOne(condition!, { $set: data! }, { session });
                            break;
                        case "delete":
                            await this.collection!.deleteOne(condition!, { session });
                            break;
                        case "increment":
                            await this.collection!.updateOne(condition!, { $inc: data! }, { session });
                            break;
                        default:
                            throw new Error(`Invalid transaction method: ${method}`);
                    }
                }
            });
            success = true;
        } catch (error) {
            console.error("Transaction failed:", error);
        } finally {
            await session.endSession();
        }
        return success;
    }
}

export default MongoDBService;
