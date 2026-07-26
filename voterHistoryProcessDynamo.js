const fs = require("fs");
const readline = require("readline");
const testFolder = "./data/voterHistory/test.json/";

fs.readdirSync(testFolder)
  .filter((file) => file.endsWith(".json"))
//   .filter((file) => file.includes("_00010_"))
  .forEach((file) => {
    const outFileStream = fs.createWriteStream(
      `./data/voterHistory/test2Dynamo/${file}`,
      { flags: "w", encoding: "utf-8" }
    );

    const rd = readline.createInterface({
      input: fs.createReadStream(`${testFolder}${file}`),
      output: false,
      console: false,
    });

    rd.on("line", function (line) {
      const object = JSON.parse(line);
      const objectDynamoHistory =
        object.voterHistory &&
        object.voterHistory.map((votingRecord) => {
          const dynamoObj = {
            M: {
              election: { S: votingRecord.election },
              party: { S: `${votingRecord.party}` },
              county: { N: `${votingRecord.county}` },
              absentee: { BOOL: votingRecord.absentee },
              provisional: { BOOL: votingRecord.provisional },
              supplemental: { BOOL: votingRecord.supplemental },
            },
          };
          Object.keys(dynamoObj.M).forEach((key) =>
            votingRecord[key] === undefined ? delete dynamoObj.M[key] : {}
          );
          return dynamoObj;
        });

      const objectDynamo = {
        id: {
          N: `${object.id}`,
        },
        absenteeDataYear: {
          N: `${object.absenteeDataYear}`,
        },
        lastName: {
          S: `${object.lastName}`,
        },
        middleName: {
          S: `${object.middleName}`,
        },
        firstName: {
          S: `${object.firstName}`,
        },
        streetNumber: {
          S: `${object.streetNumber}`,
        },
        streetName: {
          S: `${object.streetName}`,
        },
        city: {
          S: `${object.city}`,
        },
        state: {
          S: `${object.state}`,
        },
        zip: {
          S: `${object.zip}`,
        },
        countyCurrent: {
          S: `${object.countyCurrent}`,
        },
        voterHistory: {
          L: objectDynamoHistory,
        },
      };
      Object.keys(objectDynamo).forEach((key) =>
        object[key] === undefined ? delete objectDynamo[key] : {}
      );

      if (object && object.id) {
        outFileStream.write(`${JSON.stringify({"Item":objectDynamo})}\n`);
      }
    });

    rd.on("close", function () {
      // console.log('all done, son');
    });
  });
