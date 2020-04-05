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