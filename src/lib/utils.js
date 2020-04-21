const AWS = require('aws-sdk');
const iam = new AWS.IAM();

/**
 * create an http response
 * @param {string} statusCode
 * @param {string} message
 */
exports.httpResponse = (statusCode, data) => {
    return {
        headers: {
            "Content-Type": "text/html"
        },
        statusCode: statusCode,
        body: JSON.stringify({ data: data }, null, 4)
    };
};

exports.replaceAccountId = async(arn) => {
    if (arn.indexOf('123456789012') >= 0) {
        const user = await iam.getUser().promise()
        return arn.replace('123456789012', user.User.Arn.split(':')[4])
    }
    return arn
}

exports.calculateFrequencyMultiplier = () => {
    switch (process.env.FORECAST_DATA_FREQUENCY) {
        case 'Y':
            return { frequency: 'year', multiplier: 1 }
        case 'M':
            return { frequency: 'month', multiplier: 1 }
        case 'W':
            return { frequency: 'week', multiplier: 1 }
        case 'D':
            return { frequency: 'day', multiplier: 1 }
        case 'H':
            return { frequency: 'hour', multiplier: 1 }
        case '30min':
            return { frequency: 'minute', multiplier: 30 }
        case '15min':
            return { frequency: 'minute', multiplier: 15 }
        case '10min':
            return { frequency: 'minute', multiplier: 5 }
        case '5min':
            return { frequency: 'minute', multiplier: 5 }
        case '1min':
            return { frequency: 'minute', multiplier: 1 }
        default:
            break;
    }
    InvalidFrequencyError.prototype = new Error()
    throw new InvalidFrequencyError('InvalidFrequencyError', 'invalid frequency, expected Y M W D H 30min 15min 10min 5min 1min found ' + process.env.FORECAST_HORIZON)
}