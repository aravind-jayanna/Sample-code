import fastCsv from 'fast-csv';
import { PassThrough } from 'stream';
import AWS from 'aws-sdk';

const s3 = new AWS.S3();

export const handler = async (bucket, key, targetId) => {
    const data = [];
    const deletedData = [];
    const params = { Bucket: bucket, Key: key };
    try {
        const s3Stream = s3.getObject(params).createReadStream();
        await new Promise((resolve, reject) => {
            s3Stream
                .pipe(fastCsv.parse({ headers: true }))
                .on('data', (row) => {
                    if (row.Index !== targetId) {
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
            Bucket: bucket,
            Key: key,
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


const bucket = 'mt-test-bucket-v1';
const key = 'Sample-data/customers-data.csv';
const targetId = '13';

handler(bucket, key, targetId);
