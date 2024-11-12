import fastCsv from 'fast-csv';
import AWS from 'aws-sdk';
import dotenv from 'dotenv';
import { PassThrough } from 'stream';

dotenv.config();
const s3 = new AWS.S3();
const BUCKET_NAME = process.env.S3_BUCKET_NAME;
const SOURCE_S3_OBJECT_KEY = process.env.SOURCE_S3_OBJECT_KEY;
const TARGET_S3_OBJECT_KEY = process.env.TARGET_S3_OBJECT_KEY;
const PART_SIZE = 5 * 1024 * 1024; // 5 MB per part

export const handler = async (targetId) => {
    const data = [];
    const deletedData = [];
    const params = { Bucket: BUCKET_NAME, Key: SOURCE_S3_OBJECT_KEY };

    try {
        // Start time for reading from S3
        const startTime = Date.now();

        // Step 1: Read and process CSV from S3
        const s3Stream = s3.getObject(params).createReadStream();
        await new Promise((resolve, reject) => {
            s3Stream
                .pipe(fastCsv.parse({ headers: true }))
                .on('data', (row) => {
                    if (String(row.Index) === String(targetId)) {
                        deletedData.push(row);
                    } else {
                        row.Index = String(deletedData.length === 0 ? row.Index : Number(row.Index) - deletedData.length);
                        data.push(row);
                    }
                })
                .on('end', () => {
                    if (deletedData.length === 0) {
                        console.error(`Row with ID ${targetId} does not exist.`);
                        reject(`Row with ID ${targetId} does not exist.`);
                    } else {
                        resolve("Row deleted and data reindexed.");
                    }
                })
                .on('error', (error) => {
                    console.error("Error reading stream:", error);
                    reject(error);
                });
        });

        const readEndTime = Date.now();
        console.log("Time taken to read and process CSV:", (readEndTime - startTime) / 1000, "seconds");

        // Step 2: Prepare for upload and create CSV data
        const prepareUploadStartTime = Date.now();

        // Prepare multipart upload
        const { UploadId } = await s3.createMultipartUpload({
            Bucket: BUCKET_NAME,
            Key: TARGET_S3_OBJECT_KEY,
            ContentType: 'text/csv'
        }).promise();

        const uploadParts = [];
        let partNumber = 1;
        let partData = [];
        let currentPartSize = 0;

        for (let row of data) {
            const csvRow = await fastCsv.writeToString([row], { headers: partNumber === 1 });
            const csvRowBuffer = Buffer.from(csvRow);
            currentPartSize += csvRowBuffer.length;
            partData.push(csvRowBuffer);

            if (currentPartSize >= PART_SIZE) {
                // Upload the part
                const partBody = Buffer.concat(partData);
                uploadParts.push(uploadPart(s3, partBody, partNumber++, UploadId));
                partData = [];
                currentPartSize = 0;
            }
        }

        // Handle the final part
        if (partData.length > 0) {
            const partBody = Buffer.concat(partData);
            uploadParts.push(uploadPart(s3, partBody, partNumber++, UploadId));
        }

        const createAndPrepareTime = Date.now();
        console.log("Time taken to prepare for upload and create CSV data:", (createAndPrepareTime - readEndTime) / 1000, "seconds");

        // Step 3: Complete multipart upload
        const completedParts = await Promise.all(uploadParts);
        await s3.completeMultipartUpload({
            Bucket: BUCKET_NAME,
            Key: TARGET_S3_OBJECT_KEY,
            UploadId,
            MultipartUpload: { Parts: completedParts }
        }).promise();

        const endTime = Date.now();
        console.log("Time taken to upload new CSV:", (endTime - createAndPrepareTime) / 1000, "seconds");
        console.log("Total operation time:", (endTime - startTime) / 1000, "seconds");

        return { status: 'Success', message: 'CSV updated and uploaded successfully with multipart upload' };

    } catch (error) {
        console.error("Operation failed:", error);
        throw new Error("Failed to update and upload CSV.");
    }
};

// Helper function to upload each part in parallel
const uploadPart = async (s3, body, partNumber, UploadId) => {
    const { ETag } = await s3.uploadPart({
        Bucket: BUCKET_NAME,
        Key: TARGET_S3_OBJECT_KEY,
        PartNumber: partNumber,
        UploadId,
        Body: body
    }).promise();

    return { ETag, PartNumber: partNumber };
};

const targetId = "2";
handler(targetId);
