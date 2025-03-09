import MySQLService from "./my-sql-db";
import DynamoDBService from "./dynamo-db";
import MongoDBService from "./mango-db";
import DatabaseService from "./DatabaseService";

class DB {
    static getInstance(dbType: string, tableName: string): DatabaseService {
        switch (dbType) {
            case "mysql":
                return new MySQLService(tableName);
            case "dynamodb":
                return new DynamoDBService(tableName);
            case "mongodb":
                return new MongoDBService(tableName);
            default:
                throw new Error("Invalid database type selected");
        }
    }
}

export default DB;
