import fastCsv from 'fast-csv';
import { PassThrough } from 'stream';
import AWS from 'aws-sdk';
import dotenv from 'dotenv';

const s3 = new AWS.S3();

export const handler = async (targetId) => {
    dotenv.config();
    const BUCKET_NAME = process.env.S3_BUCKET_NAME;
    const OBJECT_KEY = process.env.S3_OBJECT_KEY;

    const data = [];
    const deletedData = [];
    const params = { Bucket: BUCKET_NAME, Key: OBJECT_KEY };
    try {
        const s3Stream = s3.getObject(params).createReadStream();
        await new Promise((resolve, reject) => {
            s3Stream
                .pipe(fastCsv.parse({ headers: true }))
                .on('data', (row) => {
                    if (String(row.Index) !== String(targetId)) {
                        data.push(row);
                    } else {
                        deletedData.push(row);
                    }
                })
                .on('end', () => {
                    if (deletedData.length === 0) {
                        reject(`Row with ID ${targetId} does not exist.`);
                    }
                    resolve("Row Deleted Successfully");
                })
                .on('error', reject);
        });

        const updatedCsvStream = new PassThrough();
        fastCsv.write(data, { headers: true }).pipe(updatedCsvStream);

        const uploadParams = {
            Bucket: BUCKET_NAME,
            Key: OBJECT_KEY,
            Body: updatedCsvStream,
            ContentType: 'text/csv',
        };

        await s3.upload(uploadParams).promise();

        console.log("status: 'Success', message: 'CSV updated and uploaded successfully'")
        return { status: 'Success', message: 'CSV updated and uploaded successfully' };
    } catch (error) {
        console.log("error :", error);
    }
};


const targetId = "12"
handler(targetId);
