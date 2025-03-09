import AWS from "aws-sdk";
import DatabaseService from "./DatabaseService";

class DynamoDBService extends DatabaseService {
    private dynamoDB: AWS.DynamoDB.DocumentClient;

    constructor(tableName: string) {
        super(tableName);
        this.dynamoDB = new AWS.DynamoDB.DocumentClient();
    }

    async create(data: Record<string, any>): Promise<Record<string, any>> {
        const params: AWS.DynamoDB.DocumentClient.PutItemInput = {
            TableName: this.tableName,
            Item: data,
        };
        await this.dynamoDB.put(params).promise();
        return data;
    }

    async bulkInsert(dataArray: Record<string, any>[]): Promise<boolean> {
        const params: AWS.DynamoDB.DocumentClient.BatchWriteItemInput = {
            RequestItems: {
                [this.tableName]: dataArray.map((item) => ({
                    PutRequest: { Item: item },
                })),
            },
        };
        await this.dynamoDB.batchWrite(params).promise();
        return true;
    }

    async get(condition: Record<string, any>): Promise<Record<string, any> | null> {
        const params: AWS.DynamoDB.DocumentClient.GetItemInput = {
            TableName: this.tableName,
            Key: condition,
        };
        const result = await this.dynamoDB.get(params).promise();
        return result.Item || null;
    }

    async getAll(): Promise<Record<string, any>[]> {
        const params: AWS.DynamoDB.DocumentClient.ScanInput = {
            TableName: this.tableName,
        };
        const result = await this.dynamoDB.scan(params).promise();
        return result.Items || [];
    }

    async update(
        data: Record<string, any>,
        condition: Record<string, any>
    ): Promise<boolean> {
        const updateExpression = Object.keys(data)
            .map((key) => `#${key} = :${key}`)
            .join(", ");
        const expressionAttributeNames = Object.keys(data).reduce(
            (acc, key) => ({ ...acc, [`#${key}`]: key }),
            {}
        );
        const expressionAttributeValues = Object.keys(data).reduce(
            (acc, key) => ({ ...acc, [`:${key}`]: data[key] }),
            {}
        );

        const params: AWS.DynamoDB.DocumentClient.UpdateItemInput = {
            TableName: this.tableName,
            Key: condition,
            UpdateExpression: `SET ${updateExpression}`,
            ExpressionAttributeNames: expressionAttributeNames,
            ExpressionAttributeValues: expressionAttributeValues,
            ReturnValues: "UPDATED_NEW",
        };

        await this.dynamoDB.update(params).promise();
        return true;
    }

    async delete(condition: Record<string, any>): Promise<boolean> {
        const params: AWS.DynamoDB.DocumentClient.DeleteItemInput = {
            TableName: this.tableName,
            Key: condition,
        };
        await this.dynamoDB.delete(params).promise();
        return true;
    }

    async increment(
        field: string,
        amount: number,
        condition: Record<string, any>
    ): Promise<boolean> {
        const params: AWS.DynamoDB.DocumentClient.UpdateItemInput = {
            TableName: this.tableName,
            Key: condition,
            UpdateExpression: `SET #${field} = if_not_exists(#${field}, :zero) + :amount`,
            ExpressionAttributeNames: { [`#${field}`]: field },
            ExpressionAttributeValues: { ":amount": amount, ":zero": 0 },
            ReturnValues: "UPDATED_NEW",
        };
        await this.dynamoDB.update(params).promise();
        return true;
    }

    // 🟢 Soft Delete (Mark a record as deleted instead of removing it)
    async softDelete(condition: Record<string, any>, deletedField = "isDeleted"): Promise<boolean> {
        return this.update({ [deletedField]: true }, condition);
    }

    // 🟢 Count records that match a condition
    async count(condition: Record<string, any>): Promise<number> {
        const params: AWS.DynamoDB.DocumentClient.ScanInput = {
            TableName: this.tableName,
            FilterExpression: Object.keys(condition)
                .map((key) => `#${key} = :${key}`)
                .join(" AND "),
            ExpressionAttributeNames: Object.keys(condition).reduce(
                (acc, key) => ({ ...acc, [`#${key}`]: key }),
                {}
            ),
            ExpressionAttributeValues: Object.keys(condition).reduce(
                (acc, key) => ({ ...acc, [`:${key}`]: condition[key] }),
                {}
            ),
        };

        const result = await this.dynamoDB.scan(params).promise();
        return result.Count ?? 0;
    }

    // 🟢 Check if a record exists
    async exists(condition: Record<string, any>): Promise<boolean> {
        const record = await this.get(condition);
        return !!record;
    }

    // 🟢 Find records using advanced search (LIKE, FULL TEXT SEARCH, etc.)
    async search(query: string, fields: string[]): Promise<Record<string, any>[]> {
        const filterExpressions = fields.map((field) => `contains(#${field}, :query)`).join(" OR ");

        const params: AWS.DynamoDB.DocumentClient.ScanInput = {
            TableName: this.tableName,
            FilterExpression: filterExpressions,
            ExpressionAttributeNames: fields.reduce(
                (acc, field) => ({ ...acc, [`#${field}`]: field }),
                {}
            ),
            ExpressionAttributeValues: { ":query": query },
        };

        const result = await this.dynamoDB.scan(params).promise();
        return result.Items || [];
    }

    // 🟢 Transaction Handling (For DynamoDB Transactions)
    async transaction(actions: AWS.DynamoDB.DocumentClient.TransactWriteItemList): Promise<boolean> {
        const params: AWS.DynamoDB.DocumentClient.TransactWriteItemsInput = {
            TransactItems: actions,
        };

        await this.dynamoDB.transactWrite(params).promise();
        return true;
    }
}

export default DynamoDBService;
