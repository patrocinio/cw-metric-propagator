const aws = require('aws-sdk');
const { get } = require('https');
const s3 = new aws.S3();
const tagging = new aws.ResourceGroupsTaggingAPI();

const tagCache = {};

const AWS_LAMBDA_NAMESPACE = 'AWS/Lambda';
const AWS_EC2_NAMESPACE = 'AWS/EC2';

const metricStreamToEmfMapper = (rawMetricStream) => {
    try {
        const metricStream = JSON.parse(rawMetricStream);
        let [ accountName, appName ] = metricStream.metric_stream_name.split('_');
        const metricName = metricStream.metric_name;
        const namespace = metricStream.namespace;
        const dimensionKeys = Object.keys(metricStream.dimensions);
        const dimensions = metricStream.dimensions;
        const tags = metricStream.tags || {};
        const value = metricStream.value;
        const unit = metricStream.unit;

        appName = appName || accountName;

        return {
            _aws: {
                Timestamp: metricStream.timestamp,
                CloudWatchMetrics: [
                    {
                        Namespace: `${appName}/${namespace}`,    // ${AppName}/${namespace}
                        Metrics: [
                            {
                                Name: metricName,     // ${metric_name}
                                Unit: unit,       // ${unit}
                            }
                        ],
                        Dimensions: [
                            dimensionKeys,   // All dimension names from ${dimensions}
                            [ ...dimensionKeys, "AccountName" ]  // Same as above, plus "AccountName"
                        ]
                    }
                ]
            },
            [metricName]: {
                Max: value.max,
                Min: value.min,
                Count: value.count,
                Sum: value.sum,
            },
            ...dimensions,
            ...tags,
            AccountName: accountName
        };
    } catch (e){
        console.log('Unable to parse metric stream. Error: ', e);
        return {};
    }
};

const isLambdaNamespace = (namespace, dimensions) => namespace === AWS_LAMBDA_NAMESPACE && dimensions.FunctionName;
const isEC2Namespace = (namespace, dimensions) => namespace === AWS_EC2_NAMESPACE && dimensions.InstanceId && /^i-/.test(dimensions.InstanceId);

const getTagsForArn = async(namespace, dimensions, awsPartition, region, accountId) => {
    let resourceArn = null;

    if(isLambdaNamespace(namespace, dimensions)){
        resourceArn = `arn:${awsPartition}:lambda:${region}:${accountId}:function:${dimensions.FunctionName}`;
    }

    if(isEC2Namespace(namespace, dimensions)) {
        resourceArn = `arn:${awsPartition}:ec2:${region}:${accountId}:instance/${dimensions.InstanceId}`;
    }

    if(!resourceArn) {
        return {};
    }

    let tags = tagCache[resourceArn];

    if (!tags) {
        const tagResponse = await tagging.getResources({ ResourceARNList: [resourceArn]}).promise();
        if (tagResponse.ResourceTagMappingList && tagResponse.ResourceTagMappingList.length) {
            const tagArray = tagResponse.ResourceTagMappingList[0].Tags;
            tags = {};
            for (const tagEntry of tagArray) {
                tags[tagEntry.Key] = tagEntry.Value;
            }
        }
        tagCache[resourceArn] = tags;
    }

    return tags;

};

const addTagsToFirehoseRecord = async (record, awsPartition, accountId, region) => {
    // Let's pretend there is an instance id or lambda function name in there
    const rawRecord = Buffer.from(record.data, 'base64').toString('ascii');
    const metricRecords = rawRecord.split('\n').filter((r) => r); // remove empty records
    const transformedMetricRecords = [];
    for (const metricRecord of metricRecords) {
        const metric = JSON.parse(metricRecord);
        const namespace = metric.namespace;
        const dimensions = metric.dimensions;

        const tags = await getTagsForArn(namespace, dimensions, awsPartition, region, accountId);
        if (tags) {
            metric.tags = tags;
        }

        transformedMetricRecords.push(JSON.stringify(metric));
    }
    const transformedRawRecord = transformedMetricRecords.join('\n');
    record.data = Buffer.from(transformedRawRecord + '\n', 'ascii').toString('base64');
    record.result = 'Ok'
};

exports.handler = async (event, context) => {
    // If it's Firehose sending records just send them on (later we can tag them)
    if (event.records) {
        const splitFunctionArn = context.invokedFunctionArn.split(':');
        const awsPartition = splitFunctionArn[1];
        const region = splitFunctionArn[3];
        const accountId = splitFunctionArn[4];
        for (const firehoseRecord of event.records) {
            await addTagsToFirehoseRecord(firehoseRecord, awsPartition, accountId, region);
        }
        const output = { records: event.records };
        return output
    } else if (event.Records) {
        // S3 calling ... fill in 
        // console.log(`S3 called me with ${JSON.stringify(event)}`);
        for (const s3Record of event.Records) {
            const Bucket = s3Record.s3.bucket.name;
            const Key = s3Record.s3.object.key;
            const s3Result = await s3.getObject({ Bucket, Key }).promise();
            const contents = s3Result.Body.toString();
            // console.log(`loaded ${contents.length} bytes from ${Bucket}/${Key}`);
            const metricRecords = contents.split('\n').filter((r) => r); // remove empty records
            for (const metricRecord of metricRecords) {
                // console.log(`got metric record: ${metricRecord}`);
                console.log(JSON.stringify(metricStreamToEmfMapper(metricRecord)));
            }
        }
    } else {
        console.log('Called by unknown source');
    }
};
