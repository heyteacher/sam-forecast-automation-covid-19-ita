const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const iam = new AWS.IAM();
const forecastservice = new AWS.ForecastService();
const fastCsv = require('fast-csv');
const axios = require('axios')
const moment = require('moment');
const { commitPushFiles } = require("./lib/git");
const { buildDataFiles, stringifyCSV, orderByData } = require("./lib/data_file");
const { replaceAccountId } = require("./lib/utils");

const todaySuffix = moment().format('YYYYMMDD');

/**
 * extend data and create dataset import job 
 */
exports.extendDatasetHandler = async() => {
    const dataFiles = await buildDataFiles()
    if (dataFiles.data.length > 0) {
        await commitPushFiles(dataFiles.data)
    }
    if (process.env.FORCE_CREATE_DATASET_IMPORT_JOB == "true" || dataFiles.allUpdated) {
        return {
            allUpdated: dataFiles.allUpdated,
            FORCE_CREATE_DATASET_IMPORT_JOB: process.env.FORCE_CREATE_DATASET_IMPORT_JOB
        }
    } else {
        function ToBeUpdatedError(name, message) {
            this.name = name;
            this.message = message;
        }
        ToBeUpdatedError.prototype = new Error()
        throw new ToBeUpdatedError('ToBeUpdatedError', 'dataset old to be updated')
    }
};

/** 
 * create dateset import job  
 */
exports.createDatasetImportJobHandler = async() => {
    console.log(`download dataset from ${process.env.INPUT_DATASET_URL}`)
    const responseRegion = await axios.get(process.env.INPUT_DATASET_URL);
    const regionalData = responseRegion.data

    console.log('prepare CSV for forecast dataset import')
    const data = []
    for await (const row of regionalData) {
        data.push({
            'item_id': row.denominazione_regione,
            'timestamp': moment(row.data).format('YYYY-MM-DD HH:MM:ss'),
            'target_value': row.totale_nuovi_casi
        })
    }
    // check if the last day of dataset is today
    const today = moment().format('YYYY-MM-DD');
    const lastTimestamp = data[data.length - 1].timestamp;
    console.log('region dataset last date:', lastTimestamp);
    if (process.env.FORCE_CREATE_DATASET_IMPORT_JOB != "true" && !lastTimestamp.startsWith(today)) {
        function RegionalDatasetNotUpdatedToToday(name, message) {
            this.name = name;
            this.message = message;
        }
        RegionalDatasetNotUpdatedToToday.prototype = new Error()
        throw new RegionalDatasetNotUpdatedToToday('RegionalDatasetNotUpdatedToToday', 'Dataset old, not yet updated')
    }

    const objectParams = {
        Bucket: process.env.FORECAST_INPUT_BUCKET_NAME,
        Key: process.env.FORECAST_INPUT_BUCKET_KEY,
        Body: Buffer.from(stringifyCSV(data))
    }
    console.debug(`upload  ${process.env.FORECAST_INPUT_BUCKET_NAME}/${process.env.FORECAST_INPUT_BUCKET_KEY}`)
    await s3.upload(objectParams).promise()

    const params = {
        DataSource: {
            S3Config: {
                Path: `s3://${process.env.FORECAST_INPUT_BUCKET_NAME}/${process.env.FORECAST_INPUT_BUCKET_KEY}`,
                RoleArn: await replaceAccountId(process.env.FORECAST_EXE_ROLE_ARN),
            }
        },
        DatasetArn: await replaceAccountId(process.env.FORECAST_DATASET_ARN),
        DatasetImportJobName: `${process.env.FORECAST_DATASET_IMPORT_JOB_NAME_PREFIX}_${todaySuffix}`,
        TimestampFormat: 'yyyy-MM-dd HH:mm:ss'
    };
    console.log(`create dataset import job ${params.DatasetImportJobName} on dataset ${process.env.FORECAST_DATASET_ARN} with role ${process.env.FORECAST_EXE_ROLE_ARN} `)
    return forecastservice.createDatasetImportJob(params).promise()
}

/**
 * create forecast
 */
exports.createForecastHandler = async() => {
    // create forecast with ForecastTypes: 0.5 (default was [0.1, 0.5, 0.9])
    const params = {
        ForecastName: `${process.env.FORECAST_NAME_PREFIX}_${todaySuffix}`,
        PredictorArn: await replaceAccountId(process.env.FORECAST_PREDICTOR_ARN),
        ForecastTypes: [
            '0.5',
        ]
    };
    console.log(`create forecast ${params.ForecastName}`)
    return forecastservice.createForecast(params).promise();
}

/**
 * create forecast export job
 */
exports.createForecastExportJobHandler = async() => {
    const params = {
        Destination: {
            S3Config: {
                Path: `s3://${process.env.FORECAST_OUTPUT_BUCKET_NAME}/${process.env.FORECAST_OUTPUT_BUCKET_KEY}`,
                RoleArn: await replaceAccountId(process.env.FORECAST_EXE_ROLE_ARN)
            }
        },
        ForecastArn: await replaceAccountId(`${process.env.FORECAST_ARN_PREFIX}_${todaySuffix}`),
        ForecastExportJobName: `${process.env.FORECAST_EXPORT_JOB_NAME_PREFIX}_${todaySuffix}`
    };
    console.log(`create forecast export job ${params.ForecastExportJobName}`)
    return forecastservice.createForecastExportJob(params).promise();
}

/**
 * push forecast in Github
 */
exports.pushForecastInGithubHandler = async(event) => {
    const objectParams = {
        Bucket: event.Records[0].s3.bucket.name,
        Key: event.Records[0].s3.object.key
    }
    const s3Stream = s3.getObject(objectParams).createReadStream();

    const rows = [];
    const countryRowsObjP50 = {};
    for await (const row of fastCsv.parseStream(s3Stream, { headers: true })) {
        if (!countryRowsObjP50[row.date]) {
            countryRowsObjP50[row.date] = 0
        }
        countryRowsObjP50[row.date] = countryRowsObjP50[row.date] + parseFloat(row.p50)
        rows.push(row)
    }
    console.log('region forecast dataset last date:', rows[rows.length - 1].date);

    const countryRows = Object.keys(countryRowsObjP50).map((key) => {
        return {
            date: key,
            p50: countryRowsObjP50[key],
        };
    }).sort(orderByData);
    console.log('country forecast dataset last date:', countryRows[countryRows.length - 1].date);

    const data = [{
            path: `dati-json-forecast/covid19-ita-regioni-forecast.json`,
            content: rows
        },
        {
            path: `dati-json-forecast/covid19-ita-andamento-nazionale-forecast.json`,
            content: countryRows
        }
    ]
    await commitPushFiles(data, `Amazon Forecast ${moment().format()}`)

    // clean forecast output bucket
    console.info(`clean forecast output bucket ${objectParams.Bucket}`);
    const listObjImputBucketResp = await s3.listObjects({ Bucket: objectParams.Bucket }).promise()
    for (const item of listObjImputBucketResp.Contents) {
        await s3.deleteObject({
            Bucket: objectParams.Bucket,
            Key: item.Key
        }).promise();
    }
    // clean forecast input bucket
    console.info(`clean forecast input bucket ${process.env.FORECAST_INPUT_BUCKET_NAME}`);
    const listObjOutputBucketResp = await s3.listObjects({ Bucket: process.env.FORECAST_INPUT_BUCKET_NAME }).promise()
    for (const item of listObjOutputBucketResp.Contents) {
        await s3.deleteObject({
            Bucket: process.env.FORECAST_INPUT_BUCKET_NAME,
            Key: item.Key
        }).promise();
    }
    return `forecasts pushed in github`
}

/**
 * delete dataset import Job and Export Job
 */
exports.deleteDatasetImportExportJob = async() => {
    const datasetImportJobArn = await replaceAccountId(`${process.env.FORECAST_DATASET_IMPORT_JOB_ARN_PREFIX}_${todaySuffix}`)

    const predictor = await exports.describePredictorHandler()

    // skip deletion of the first dataset import job because is in use of predictor
    if (predictor.DatasetImportJobArns.indexOf(datasetImportJobArn) < 0) {
        console.log(`delete dataset import job ${datasetImportJobArn}`)
        await forecastservice.deleteDatasetImportJob({
            DatasetImportJobArn: datasetImportJobArn,
        }).promise()
    } {
        console.log(`skip deletion of first (predictor in use) dataset import job  ${datasetImportJobArn}`)
    }
    // delete forecast export
    return deleteForecastExportJob()
}

/**
 * delete forecast export job 
 */
const deleteForecastExportJob = async() => {
    // delete forecast export job
    const forecastExportJobArn = await replaceAccountId(`${process.env.FORECAST_EXPORT_JOB_ARN_PREFIX}/${process.env.FORECAST_NAME_PREFIX}_${todaySuffix}/${process.env.FORECAST_EXPORT_JOB_NAME_PREFIX}_${todaySuffix}`)
    console.log(`delete forecast export job ${forecastExportJobArn}`)
    return forecastservice.deleteForecastExportJob({
        ForecastExportJobArn: forecastExportJobArn,
    }).promise()
}

/**
 * delete forecast
 */
exports.deleteForecastHandler = async() => {
    const forecastArn = `${process.env.FORECAST_ARN_PREFIX}_${todaySuffix}`
    console.log(`delete forecast ${forecastArn}`)
    return forecastservice.deleteForecast({
        ForecastArn: forecastArn,
    }).promise()
}

/**
 * describe dataset, error if doesn't exists
 */
exports.describeDatasetHandler = async() => {
    const datasetArn = await replaceAccountId(process.env.FORECAST_DATASET_ARN)
    console.log(`describe dataset ${datasetArn}`)
    return forecastservice.describeDataset({
        DatasetArn: datasetArn,
    }).promise()
}

/**
 * create Dataset (and associated datasetgroup)
 */
exports.createDatasetHandler = async() => {
    let createDatasetResp = null
    try {
        const paramsDataset = {
            DatasetName: process.env.FORECAST_DATASET_NAME,
            DatasetType: 'TARGET_TIME_SERIES',
            DataFrequency: 'D',
            Domain: 'CUSTOM',
            Schema: {
                Attributes: [{
                        AttributeName: "item_id",
                        AttributeType: "string"
                    },
                    {
                        AttributeName: "timestamp",
                        AttributeType: "timestamp"
                    },
                    {
                        AttributeName: "target_value",
                        AttributeType: "float"
                    }
                ]
            },
        };
        createDatasetResp = await forecastservice.createDataset(paramsDataset).promise()
            // create dataset group
        const paramsDatasetGroup = {
            DatasetGroupName: `${process.env.FORECAST_DATASET_GROUP_NAME}`,
            Domain: 'CUSTOM',
            DatasetArns: [
                createDatasetResp.DatasetArn
            ]
        };
        console.log(`create dataset group ${paramsDatasetGroup.DatasetGroupName}`)
        return await forecastservice.createDatasetGroup(paramsDatasetGroup).promise()
    } catch (err) {
        // if error and dataset was created, delete it 
        if (createDatasetResp) {
            console.log(`createDatasetGroup fails, delete dataset arn ${createDatasetResp.DatasetArn}`)
            await forecastservice.deleteDataset({
                DatasetArn: createDatasetResp.DatasetArn
            }).promise()
        }
        throw err
    }
}

/**
 * describe predictor, error if doesn't exists
 */
exports.describePredictorHandler = async() => {
    const predictorArn = await replaceAccountId(process.env.FORECAST_PREDICTOR_ARN)
    console.log(`describe predactor ${predictorArn}`)
    return forecastservice.describePredictor({
        PredictorArn: predictorArn
    }).promise()
}

/**
 * create the predictor
 */
exports.createPredictorHandler = async() => {
    const datasetGroupArn = await replaceAccountId(process.env.FORECAST_DATASET_GROUP_ARN)
    const params = {
        FeaturizationConfig: {
            ForecastFrequency: 'D',
        },
        ForecastHorizon: '8',
        InputDataConfig: {
            DatasetGroupArn: datasetGroupArn,
        },
        PredictorName: process.env.FORECAST_PREDICTOR_NAME,
        PerformAutoML: true
    };
    return await forecastservice.createPredictor(params).promise()
}