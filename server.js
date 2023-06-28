const grpc = require("@grpc/grpc-js");
const protoLoader = require('@grpc/proto-loader');
const axios = require("axios");
const packageDefinition = protoLoader.loadSync('computeandstorage.proto');
const server = new grpc.Server();
const computeAndStoragePackage = grpc.loadPackageDefinition(packageDefinition).computeandstorage;
const AWS = require('aws-sdk');
AWS.config.update({
  accessKeyId: '',
  secretAccessKey: '',
  region: 'us-east-1',
});
const s3 = new AWS.S3();

function storeData(call, callback) {
  const data = call.request.data;
  const bucketName = 'cloud-assignment-2-bansari';
  const fileName = 'dataFromRobApp.txt'; 

  const params = {
    Bucket: bucketName,
    Key: fileName,
    Body: data
  };

  s3.upload(params, (err, data) => {
    if (err) {
      console.error('Error storing data in S3:', err);
      callback(err);
    } else {
      const s3uri = data.Location;
      const response = { s3uri };
      callback(null, response);
    }
  });
}

function appendData(call, callback) {
  const data = call.request.data;
  const bucketName = 'cloud-assignment-2-bansari';
  const fileName = 'dataFromRobApp.txt';

  s3.getObject({ Bucket: bucketName, Key: fileName }, (err, getObjectData) => {
    if (err) {
      console.error('Error retrieving existing file content from S3:', err);
      callback(err);
    } else {
      const existingData = getObjectData.Body.toString();
      const newData = existingData + data;

      s3.putObject({ Bucket: bucketName, Key: fileName, Body: newData }, (err, putObjectData) => {
        if (err) {
          console.error('Error appending data to S3 file:', err);
          callback(err);
        } else {
          const s3uri = `https://${bucketName}.s3.amazonaws.com/${fileName}`;
          const response = { s3uri };
          callback(null, response);
        }
      });
    }
  });
}

function extractFileNameFromS3Uri(s3uri) {
  const parts = s3uri.split('/');
  return parts[parts.length - 1];
}

function deleteFile(call, callback) {
  const s3uri = call.request.s3uri;
  const bucketName = 'cloud-assignment-2-bansari';
  const fileName = extractFileNameFromS3Uri(s3uri);

  const params = {
    Bucket: bucketName,
    Key: fileName,
  };

  s3.deleteObject(params, (err, data) => {
    if (err) {
      console.error('Error deleting file from S3:', err);
      callback(err);
    } else {
      callback(null, {});
    }
  });
}

function main() {
  server.addService(computeAndStoragePackage.EC2Operations.service, { StoreData: storeData, AppendData: appendData, DeleteFile: deleteFile });
  server.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), async () => {
    server.start();
    try {
      const res = await axios.post('http://54.173.209.76:9000/start', {
        banner: 'B00910696',
        ip: '44.201.126.120:50051',
      });
      console.log(res);
    } catch (err) {
      console.log(err);
    }
  });
}

main();
