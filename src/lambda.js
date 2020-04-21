const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const forecastservice = new AWS.ForecastService();
const fastCsv = require('fast-csv');
const axios = require('axios')
const moment = require('moment');
const { commitPushFiles } = require("./lib/git");
const { buildDataFiles, stringifyCSV, orderByData } = require("./lib/data_file");
const { replaceAccountId, calculateFrequencyMultiplier } = require("./lib/utils");

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
    console.log(`create dataset import job ${params.DatasetImportJobName} on dataset ${params.DatasetArn} with role ${params.DataSource.S3Config.RoleArn} `)
    await forecastservice.createDatasetImportJob(params).promise()
    return createDatasetImportJobRelated(regionalData)
}

/** 
 * create dateset import job related
 */
const createDatasetImportJobRelated = async(regionalData) => {

    const { frequency, multiplier } = calculateFrequencyMultiplier();
    const windowInfTimestamp = moment(regionalData[regionalData.length - 1].data)
        .add(-1 * multiplier * process.env.FORECAST_HORIZON,
            frequency);
    console.log('prepare CSV for forecast dataset related import')
    const data = []
    for await (const row of regionalData) {
        data.push({
                'item_id': row.denominazione_regione,
                'timestamp': moment(row.data).format('YYYY-MM-DD HH:MM:ss'),
                'related_value': row.tamponi
            })
            // copy missing data from past in order to cover the orizon
        if (!moment(row.data).isBefore(windowInfTimestamp)) {
            const horizonToAdd = (multiplier * process.env.FORECAST_HORIZON) + multiplier
            data.push({
                'item_id': row.denominazione_regione,
                'timestamp': moment(row.data).add(horizonToAdd, frequency).format('YYYY-MM-DD HH:MM:ss'),
                'related_value': row.tamponi
            })
        }
    }
    const objectParams = {
        Bucket: process.env.FORECAST_INPUT_BUCKET_NAME,
        Key: process.env.FORECAST_RELATED_INPUT_BUCKET_KEY,
        Body: Buffer.from(stringifyCSV(data))
    }
    console.debug(`upload  ${process.env.FORECAST_INPUT_BUCKET_NAME}/${process.env.FORECAST_RELATED_INPUT_BUCKET_KEY}`)
    await s3.upload(objectParams).promise()

    const params = {
        DataSource: {
            S3Config: {
                Path: `s3://${process.env.FORECAST_INPUT_BUCKET_NAME}/${process.env.FORECAST_RELATED_INPUT_BUCKET_KEY}`,
                RoleArn: await replaceAccountId(process.env.FORECAST_EXE_ROLE_ARN),
            }
        },
        DatasetArn: await replaceAccountId(process.env.FORECAST_RELATED_DATASET_ARN),
        DatasetImportJobName: `${process.env.FORECAST_RELATED_DATASET_IMPORT_JOB_NAME_PREFIX}_${todaySuffix}`,
        TimestampFormat: 'yyyy-MM-dd HH:mm:ss'
    };
    console.log(`create dataset import job ${params.DatasetImportJobName} on dataset ${params.DatasetArn} with role ${params.DataSource.S3Config.RoleArn} `)
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

    const suffix = process.env.FORECAST_ALGORITHM_ARN ?
        process.env.FORECAST_ALGORITHM_ARN.replace('arn:aws:forecast:::algorithm/', '-') :
        ''
    const data = [{
            path: `dati-json-forecast/covid19-ita-regioni-forecast${suffix}.json`,
            content: rows
        },
        {
            path: `dati-json-forecast/covid19-ita-andamento-nazionale-forecast${suffix}.json`,
            content: countryRows
        }
    ]
    await commitPushFiles(data, `Amazon-Forecast${suffix} ${moment().format()}`)

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
        console.log(`skip deletion of first two (predictor in use) dataset import job  ${datasetImportJobArn}`)
    }

    const relatedDatasetImportJobArn = await replaceAccountId(`${process.env.FORECAST_RELATED_DATASET_IMPORT_JOB_ARN_PREFIX}_${todaySuffix}`)

    // skip deletion of the first dataset import job because is in use of predictor
    if (predictor.DatasetImportJobArns.indexOf(relatedDatasetImportJobArn) < 0) {
        console.log(`delete dataset import job ${relatedDatasetImportJobArn}`)
        await forecastservice.deleteDatasetImportJob({
            DatasetImportJobArn: relatedDatasetImportJobArn,
        }).promise()
    } {
        console.log(`skip deletion of first two (predictor in use) dataset import job  ${relatedDatasetImportJobArn}`)
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
    const forecastArn = await replaceAccountId(`${process.env.FORECAST_ARN_PREFIX}_${todaySuffix}`)
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
    let createRelatedDatasetResp = null
    try {
        const paramsDataset = {
            DatasetName: process.env.FORECAST_DATASET_NAME,
            DatasetType: 'TARGET_TIME_SERIES',
            DataFrequency: process.env.FORECAST_DATA_FREQUENCY,
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

        // create the related dataset
        createRelatedDatasetResp = await createRelatedDatasetHandler();

        // create dataset group
        const paramsDatasetGroup = {
            DatasetGroupName: `${process.env.FORECAST_DATASET_GROUP_NAME}`,
            Domain: 'CUSTOM',
            DatasetArns: [
                createDatasetResp.DatasetArn,
                createRelatedDatasetResp.DatasetArn
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
        // if error and related dataset was created, delete it 
        if (createRelatedDatasetResp) {
            console.log(`createDatasetG fails, delete dataset arn ${createRelatedDatasetResp.DatasetArn}`)
            await forecastservice.deleteDataset({
                DatasetArn: createRelatedDatasetResp.DatasetArn
            }).promise()
        }
        throw err
    }
}

/**
 * create Related Dataset
 */
const createRelatedDatasetHandler = async() => {
    const paramsDataset = {
        DatasetName: process.env.FORECAST_RELATED_DATASET_NAME,
        DatasetType: 'RELATED_TIME_SERIES',
        DataFrequency: process.env.FORECAST_DATA_FREQUENCY,
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
                    AttributeName: "related_value",
                    AttributeType: "float"
                }
            ]
        },
    };
    const createRelatedDatasetResp = await forecastservice.createDataset(paramsDataset).promise()
    return createRelatedDatasetResp
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
    const isEmptyForecast = !process.env.FORECAST_ALGORITHM_ARN || process.env.FORECAST_ALGORITHM_ARN == ''
    const params = {
        FeaturizationConfig: {
            ForecastFrequency: process.env.FORECAST_DATA_FREQUENCY,
        },
        ForecastHorizon: process.env.FORECAST_HORIZON,
        InputDataConfig: {
            DatasetGroupArn: datasetGroupArn,
        },
        PredictorName: process.env.FORECAST_PREDICTOR_NAME,
        PerformAutoML: isEmptyForecast // Perform Auto ML if no algorithm is specified
    };
    if (!isEmptyForecast) {
        params.AlgorithmArn = process.env.FORECAST_ALGORITHM_ARN
    }
    console.log(`create predictor params`, params)
    return await forecastservice.createPredictor(params).promise()
}