const fs = require("fs");
const readline = require("readline");
const testFolder = "./data/voterHistory/county_city_street.json/";

fs.readdirSync(testFolder)
  .filter((file) => file.endsWith(".json"))
  //   .filter((file) => file.includes("_00010_"))
  .forEach((file) => {
    const outFileStream = fs.createWriteStream(
      `./data/voterHistory/county_city_street.json_dynamo/${file}`,
      { flags: "w", encoding: "utf-8" }
    );

    const rd = readline.createInterface({
      input: fs.createReadStream(`${testFolder}${file}`),
      output: false,
      console: false,
    });

    const idLengths = [];

    rd.on("line", function (line) {
      const object = JSON.parse(line);
      const objectDynamoVoters =
        object.voters &&
        object.voters.map((voter) => {
          const dynamoObj = {
            M: {
              id: { N: `${voter.id}` },
              firstName: { S: `${voter.firstName}` },
              lastName: { S: `${voter.lastName}` },
              electionLastVoted: { S: `${voter.electionLastVoted}` },
            },
          };
          Object.keys(dynamoObj.M).forEach((key) =>
            voter[key] === undefined ? delete dynamoObj.M[key] : {}
          );
          return dynamoObj;
        });

      const id = `${object.countyCurrent}_${object.city}_${object.streetName}`;
      idLengths.push({id, length: object.voters.length});
      object.id = id;

      const objectDynamo = {
        id: {
          S: id,
        },
        countyCurrent: {
          S: `${object.countyCurrent}`,
        },
        city: {
          S: `${object.city}`,
        },
        streetName: {
          S: `${object.streetName}`,
        },
        voters: {
          L: objectDynamoVoters,
        },
      };
      Object.keys(objectDynamo).forEach((key) =>
        object[key] === undefined ? delete objectDynamo[key] : {}
      );

      if (object) {
        outFileStream.write(`${JSON.stringify({ Item: objectDynamo })}\n`);
      }
    });

    rd.on("close", function () {
        console.log(idLengths.filter(row => row.length>1000));
    });
  });
