// main.js implements a sample node.js application built on InfluxDB.
//
// This application is designed to illustrate the use of the influxdb-client
// module and the facilities of the underlying database; in some cases it omits
// important best practices such as handling errors and authenticating requests.
// Be sure to include those things in any real-world production application!

require("dotenv").config();
const { InfluxDB, HttpError, Point } = require("@influxdata/influxdb-client");
const { OrgsAPI, BucketsAPI } = require("@influxdata/influxdb-client-apis");
const request = require("request");
// organizationName specifies your InfluxDB organization.
// Organizations are used by InfluxDB to group resources such as users,
// tasks, buckets, dashboards and more.
const organizationName = process.env.INFLUXDB_ORGANIZATION;

// organizationID is used by the task API and is populated
// by looking up the organizationName at startup.
const organizationID = process.env.ORGANIZATION_ID;

// url is the URL of your InfluxDB instance or Cloud environment.
// This is also the URL where you reach the UI for your account.
const url = process.env.INFLUXDB_HOST;

// token appropriately scoped to access the resources needed by your app.
// For ease of use in this example, you should use an "all access" token.
// In a production application, you should use a properly scoped token to
// access only the resources needed by your application and store it securely.
// More information about permissions and tokens can be found here:
// https://docs.influxdata.com/influxdb/v2.1/security/tokens/
const token = process.env.INFLUXDB_TOKEN;

// bucketName specifies an InfluxDB bucket in your organization.
// A bucket is where you store data, and you can group related data into a bucket.
// You can also scope permissions to the bucket level as well.
const bucketName = process.env.INFLUXDB_BUCKET;

// client for accessing InfluxDB
const client = new InfluxDB({ url, token });
const writeAPI = client.getWriteApi(organizationName, bucketName);
const queryClient = client.getQueryApi(organizationName);

// set up your server and begin listening on port 8080.
const express = require("express");
const app = express();
const PORT = 8080;
const bodyParser = require("body-parser");

app.use(bodyParser.json());

app.use("/", (req, res, next) => {
  console.log("A new request was received at " + Date.now());
  next();
});

// Register some routes for your application. Check out the documentation of
// each function registered below for more details on how it works.

app.get("/", (req, res) => {
  res.send("Welcome to your first InfluxDB Application!!");
});

// ingest data for a user to InfluxDB.
//
// Note that "user" here refers to a user in your application, not an InfluxDB user.
//
// POST the following data to the /ingest endpoint to test this function:
// {"user_id":"user1", "measurement":"measurement1","field1":1.0}
//
// A point requires at a minimum: A measurement, a field, and a value.
// Where a bucket is similar to a database in a relational database, a measurement is similar
// to a table and a field and its related value are similar to a column and value.
// The user_id will be used to "tag" each point, so that your queries can easily find the
// data for each separate user.
//
// You can write any number of tags and fields in a single point, but only one measurement
// To understand how measurements, tag values, and fields define points and series, follow this link:
// https://awesome.influxdata.com/docs/part-2/influxdb-data-model/
//
// For learning about how to ingest data at scale, and other details, follow this link:
// https://influxdb-client.readthedocs.io/en/stable/usage.html#write

app.post("/ingest", (req, res) => {
  const user_id = req.body.user_id;
  const measurement = req.body.measurement;
  const value = req.body.field1;

  // Construct an InfluxDB point from the JSON request suitable for writing.
  let point = new Point(measurement)
    .tag("user_id", user_id)
    .floatField("field1", value)
    .timestamp(new Date());
  try {
    // Write the point to InfluxDB using write API.
    writeAPI.writePoint(point);
    writeAPI.close().then(() => {
      console.log("WRITE FINISHED");
    });
    res.sendStatus(200);
  } catch (e) {
    if (e.res.statusCode === 401) {
      res.send("error: insufficient permission");
    }
    if (e.res.statusCode === 404) {
      res.send("Bucket name does not exist");
    }
  }
  // You can view the data written by this function by navigating to
  // the InfluxDB UI for your account and using the Data Explorer.
});

// query serves down sampled data for a user in JSON format. It returns the last
// value for each field of the data, returning the latest min, max and mean value
// within the last 24 hours.
//
// Note that "user" here refers to a user in your application, not an InfluxDB user.
//
// POST the following to test this endpoint:
// {"user_id":"user1"}
app.post("/query", (req, res) => {
  const user_id = req.body.user_id;

  // Queries can be written in either Flux or InfluxQL.
  // Here we use a parameterized Flux query.
  //
  // Simple queries are in the format of from() |> range() |> filter()
  // Flux can also be used to do complex data transformations as well as integrations.
  // Follow this link to learn more about using Flux:
  // https://awesome.influxdata.com/docs/part-2/introduction-to-flux/
  const params = {
    bucket_name: bucketName,
    user_id: user_id,
  };
  let fluxQuery = `from(bucket: "${params.bucket_name}")
    |> range(start: -24h)
    |> filter(fn: (r) => r._measurement == "downsampled")
    |> filter(fn: (r) => r.user_id == "${params.user_id}")
    |> last()`;

  // The query API offers the ability to retrieve raw data via QueryRow and QueryRowWithParams, or
  // a parsed representation via Query and QueryWithParams. We use the former here.

  queryClient.queryRows(fluxQuery, {
    next: (row, tableMeta) => {
      const tableObject = tableMeta.toObject(row);
      console.log(tableObject);
    },
    error: (error) => {
      console.error("\nError", error);
    },
    complete: () => {
      console.log("\nSuccess");
    },
  });
  res.sendStatus(200);
});

// recreateBucket creates a new bucket. If a bucket of the same name already exists,
// it is deleted and then created again.

async function recreateBucket(name) {
  const orgsAPI = new OrgsAPI(client);
  const organizations = await orgsAPI.getOrgs({ organizationName });
  if (!organizations || !organizations.orgs || !organizations.orgs.length) {
    console.error(`No organization named "${organizationName}" found!`);
  }
  const orgID = organizations.orgs[0].id;
  console.log(
    `Using organization "${organizationName}" identified by "${orgID}"`
  );

  console.log("*** Get buckets by name ***");
  const bucketsAPI = new BucketsAPI(client);
  try {
    const buckets = await bucketsAPI.getBuckets({ orgID, name });
    if (buckets && buckets.buckets && buckets.buckets.length) {
      console.log(`Bucket named "${name}" already exists"`);
      const bucketID = buckets.buckets[0].id;
      console.log(
        `*** Delete Bucket "${name}" identified by "${bucketID}" ***`
      );
      await bucketsAPI.deleteBucketsID({ bucketID });
    }
  } catch (e) {
    if (e instanceof HttpError && e.statusCode == 404) {
      // OK, bucket not found
    } else {
      throw e;
    }
  }

  console.log(`*** Create Bucket "${name}" ***`);
  // creates a bucket, entity properties are specified in the "body" property
  const bucket = await bucketsAPI.postBuckets({ body: { orgID, name } });
  console.log(
    JSON.stringify(
      bucket,
      (key, value) => (key === "links" ? undefined : value),
      2
    )
  );
}

// tasks creates a task owned by the requested user that will down sample their data and find any values in the specified time range that have a
// value of 0.0 and will copy those points into a special bucket.
//
// Note that "user" here refers to a user in your application, not an InfluxDB user.

// POST the following to test this endpoint:
// {"user_id":"user1"}
app.post("/tasks", (req, res) => {
  // ensure there is a bucket to copy the data into
  recreateBucket("processed_data_bucket");
  const user_id = req.body.user_id;

  //  The follow flux will find any values in the specified time range that have a
  //  value of 0.0 and will copy those points into a special bucket.
  //  This demonstrates 2 concepts:
  //  1. "downsampling", or the ability to easily precompute data so that you can supply low latency
  //     queries for your UI.
  //     For more on downsampling, see:
  //     https://awesome.influxdata.com/docs/part-2/querying-and-data-transformations/#materialized-views-or-downsampling-tasks
  //  2. "alerting", or the ability to send a notification based on certain values and conditions.
  //     For example, rather than writing the data to a new bucket, you can use http.post() to call back your application
  //     or a different service.
  //     To see the full power of the alerting system, see:
  //     https://awesome.influxdata.com/docs/part-3/checks-and-notifications/

  const query = `option task = {name: "${user_id}_task", every: 1m}
  from(bucket: "${bucketName}")
  |> range(start: -1m)
  |> filter(fn: (r) => r.user_id == "${user_id}")
  |> filter(fn: (r) => r._value == 0.0)
  |> to(bucket: "processed_data_bucket")`;

  // Your real code should authorize the user, and ensure that the user_id matches the authorization.
  const data = {
    flux: query,
    org: organizationID,
    status: "active",
    description: "This task downsamples",
  };
  const host = url + "/api/v2/tasks";
  console.log("host: ", host);

  request.post(
    {
      url: host,
      body: data,
      headers: {
        Authorization: `Token ${token}`,
        "Content-Type": "application/json",
      },
      json: true,
    },
    function (error, response, body) {
      if (error) {
        return console.log(error);
      }
      res.sendStatus(200);
      console.log(`Status: ${response.statusCode}`);
      console.log(body);
    }
  );
});

// Serve the routes configured above on port 8080.
// Note: a real-world
// production app exposed on the internet should use a server with properly
// configured timeouts, certificates, etc.

app.listen(PORT, () => {
  console.log("listening on port 8080");
});
