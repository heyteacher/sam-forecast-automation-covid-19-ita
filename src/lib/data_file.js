const axios = require('axios'),
    moment = require('moment'),
    createCsvStringifier = require('csv-writer').createObjectCsvStringifier,
    AWS = require('aws-sdk'),
    ses = new AWS.SES(),
    Twitter = require('twitter');

exports.stringifyCSV = (data) => {
    const header = {
        header: []
    }
    for (const key in data[0]) {
        header.header.push({
            id: key,
            title: key
        })
    }
    const csvStringifier = createCsvStringifier(header);
    return csvStringifier.getHeaderString() + csvStringifier.stringifyRecords(data);
}

exports.buildDataFiles = async(day) => {

    const originCountryUrl = `https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-json/dpc-covid19-ita-andamento-nazionale.json`
    const targetCountryUrl = `https://raw.githubusercontent.com/heyteacher/COVID-19/master/dati-json/dpc-covid19-ita-andamento-nazionale.json`

    const originRegionUrl = `https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-json/dpc-covid19-ita-regioni.json`
    const targetRegionUrl = `https://raw.githubusercontent.com/heyteacher/COVID-19/master/dati-json/dpc-covid19-ita-regioni.json`

    const originProvinceUrl = `https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-json/dpc-covid19-ita-province.json`
    const targetProvinceUrl = `https://raw.githubusercontent.com/heyteacher/COVID-19/master/dati-json/dpc-covid19-ita-province.json`

    const originResponseCountry = await axios.get(originCountryUrl);
    const targetResponseCountry = await axios.get(targetCountryUrl);

    const originResponseRegion = await axios.get(originRegionUrl);
    const targetResponseRegion = await axios.get(targetRegionUrl);

    const originResponseProv = await axios.get(originProvinceUrl);
    const targetResponseProv = await axios.get(targetProvinceUrl);

    const originCountryDate = getLastValue(originResponseCountry.data);
    let targetCountryDate = getLastValue(targetResponseCountry.data)

    const originRegionDate = getLastValue(originResponseRegion.data);
    let targetRegionDate = getLastValue(targetResponseRegion.data);
    const originProvDate = getLastValue(originResponseProv.data)
    let targetProvDate = getLastValue(targetResponseProv.data)

    const data = []
    const today = moment().format('YYYY-MM-DD')
    let regionalData = null
    if (process.env.FORCE_EXTEND_DATA == 'true' || originCountryDate != targetCountryDate) {
        targetCountryDate = originCountryDate
        const countryData = extendData(originResponseCountry.data)
        if (!day) {
            data.push({
                path: `dati-json/dpc-covid19-ita-andamento-nazionale.json`,
                content: countryData
            }, {
                path: `dati-andamento-nazionale/dpc-covid19-ita-andamento-nazionale-${moment(originCountryDate).format('YYYYMMDD')}.csv`,
                content: exports.stringifyCSV(getDailyRows(countryData))
            }, {
                path: 'dati-andamento-nazionale/dpc-covid19-ita-andamento-nazionale-latest.csv',
                content: exports.stringifyCSV(getDailyRows(countryData))
            }, {
                path: 'dati-json/dpc-covid19-ita-andamento-nazionale-latest.json',
                content: getDailyRows(countryData)
            }, {
                path: `dati-andamento-nazionale/dpc-covid19-ita-andamento-nazionale.csv`,
                content: exports.stringifyCSV(countryData)
            })
        } else {
            const suffix = moment(day).format('YYYYMMDD')
            data.push({
                path: `dati-andamento-nazionale/dpc-covid19-ita-andamento-nazionale-${suffix}.csv`,
                content: exports.stringifyCSV(getDailyRows(countryData, day))
            })
        }
    } else {
        console.info("country dataset already updated");
    }
    if (process.env.FORCE_EXTEND_DATA == 'true' || originRegionDate != targetRegionDate) {
        targetRegionDate = originRegionDate
        regionalData = extendData(originResponseRegion.data.sort(orderByRegionData)).sort(orderByDataRegion)
        if (!day) {
            data.push({
                path: `dati-json/dpc-covid19-ita-regioni.json`,
                content: regionalData
            }, {
                path: `dati-regioni/dpc-covid19-ita-regioni-${moment(originRegionDate).format('YYYYMMDD')}.csv`,
                content: exports.stringifyCSV(getDailyRows(regionalData))
            }, {
                path: 'dati-regioni/dpc-covid19-ita-regioni-latest.csv',
                content: exports.stringifyCSV(getDailyRows(regionalData))
            }, {
                path: 'dati-json/dpc-covid19-ita-regioni-latest.json',
                content: getDailyRows(regionalData)
            }, {
                path: `dati-regioni/dpc-covid19-ita-regioni.csv`,
                content: exports.stringifyCSV(regionalData)
            })
        } else {
            const suffix = moment(day).format('YYYYMMDD')
            data.push({
                path: `dati-regioni/dpc-covid19-ita-regioni-${suffix}.csv`,
                content: exports.stringifyCSV(getDailyRows(regionalData, day))
            })
        }
    } else {
        console.info("regional dataset already updated");
    }
    if (process.env.FORCE_EXTEND_DATA == 'true' || originProvDate != targetProvDate) {
        targetProvDate = originProvDate
        const provinceData = extendData(originResponseProv.data.sort(orderByProvinceData), true).sort(orderByDataProvince)
        if (!day) {
            data.push({
                path: `dati-json/dpc-covid19-ita-province.json`,
                content: provinceData
            }, {
                path: `dati-province/dpc-covid19-ita-province-${moment(originProvDate).format('YYYYMMDD')}.csv`,
                content: exports.stringifyCSV(getDailyRows(provinceData))
            }, {
                path: 'dati-province/dpc-covid19-ita-province-latest.csv',
                content: exports.stringifyCSV(getDailyRows(provinceData))
            }, {
                path: 'dati-json/dpc-covid19-ita-province-latest.json',
                content: getDailyRows(provinceData)
            }, {
                path: `dati-province/dpc-covid19-ita-province.csv`,
                content: exports.stringifyCSV(provinceData)
            });
        } else {
            const suffix = moment(day).format('YYYYMMDD')
            data.push({
                path: `dati-province/dpc-covid19-ita-province-${suffix}.csv`,
                content: exports.stringifyCSV(getDailyRows(provinceData, day))
            });
        }
    } else {
        console.info("province dataset already updated");
    }
    console.info(
        "country updated",
        targetCountryDate == today,
        "region updated",
        targetRegionDate == today,
        "province updated",
        targetProvDate == today);

    const allUpdated = targetCountryDate == today &&
        targetRegionDate == today &&
        targetProvDate == today

    if ((allUpdated && data.length > 0) || process.env.FORCE_EXTEND_DATA == 'true') {
        await mailAndTweetData(originResponseCountry.data, originResponseRegion.data, originResponseProv.data)
    }
    return {
        allUpdated: allUpdated,
        data: data
    }
}


const mailAndTweetData = async(countryData, regionalData, provinceData) => {
    if (!process.env.SES_IDENTITY_NAME || process.env.SES_IDENTITY_NAME.trim() == "") {
        return
    }
    const dailyCountryData = getDailyRows(extendData(countryData))[0]
    const dailyRegionData = getDailyRows(extendData(regionalData.filter(row => row.denominazione_regione === process.env.SES_REGION_DATA)))[0]
    const dailyProvinceData = getDailyRows(extendData(provinceData.filter(row => row.denominazione_provincia === process.env.SES_PROVINCE_DATA), true))[0]

    const numberFormatTotal = new Intl.NumberFormat("en-GB", { style: "decimal" });
    const numberFormatNew = new Intl.NumberFormat("en-GB", { style: "decimal", signDisplay: 'always' });
    const textCountry = `COVID-19 ITA Daily Report

New Confirmed: ${numberFormatNew.format(dailyCountryData.totale_nuovi_casi)} (${Math.round(dailyCountryData.totale_nuovi_casi / dailyCountryData.nuovi_casi_testati * 10000) / 100}% on New People Tested) 
New Tests: ${numberFormatNew.format(dailyCountryData.nuovi_tamponi)}
New People Tested: ${numberFormatNew.format(dailyCountryData.nuovi_casi_testati)}
New Hospitalized: ${numberFormatNew.format(dailyCountryData.nuovi_ricoverati_con_sintomi)}
Total Hospitalized: ${numberFormatTotal.format(dailyCountryData.ricoverati_con_sintomi)}
New Intensive Care: ${numberFormatNew.format(dailyCountryData.nuovi_terapia_intensiva)}
Total Intensive Care: ${numberFormatTotal.format(dailyCountryData.terapia_intensiva)}
New Deads: ${numberFormatNew.format(dailyCountryData.nuovi_deceduti)}

`

    const textRegion = `COVID-19 ${process.env.SES_REGION_DATA}  Daily Update
New Confirmed: ${numberFormatNew.format(dailyRegionData.totale_nuovi_casi)} (${Math.round(dailyRegionData.totale_nuovi_casi / dailyRegionData.nuovi_casi_testati * 10000) / 100}% on New People Tested)
New Tests: ${numberFormatNew.format(dailyRegionData.nuovi_tamponi)} 
New People Tested: ${numberFormatNew.format(dailyRegionData.nuovi_casi_testati)}
New Hospitalized: ${numberFormatNew.format(dailyRegionData.nuovi_ricoverati_con_sintomi)}
Total Hospitalized: ${numberFormatTotal.format(dailyRegionData.ricoverati_con_sintomi)}
New Intensive care: ${numberFormatNew.format(dailyRegionData.nuovi_terapia_intensiva)}
Total Intensive care: ${numberFormatTotal.format(dailyRegionData.terapia_intensiva)}
Daily Deads: ${numberFormatNew.format(dailyRegionData.nuovi_deceduti)}

`
    const textProvince = `COVID-19 ${process.env.SES_PROVINCE_DATA} daily update
daily confirmed: ${numberFormatNew.format(dailyProvinceData.totale_nuovi_casi)}

`

    const textURL = `
https://heyteacher.github.io/COVID-19/#/`

    const params = {
        Destination: {
            ToAddresses: [process.env.SES_IDENTITY_NAME]
        },
        Message: {
            Body: {
                Text: {
                    Data: `${textCountry}${textRegion}${textProvince}${textURL}`
                }
            },
            Subject: { Data: "COVID-19 daily update" }
        },
        Source: process.env.SES_IDENTITY_NAME
    };
    try {
        const data = await ses.sendEmail(params).promise()
        console.info("sendEmail to", process.env.SES_IDENTITY_NAME, "MessageId", data.MessageId)
    } catch (error) {
        console.error('sendEmail error', error)
    }

    // tweet daily update
    try {
        var client = new Twitter({
            consumer_key: process.env.TWITTER_CONSUMER_KEY,
            consumer_secret: process.env.TWITTER_CONSUMER_SECRET,
            access_token_key: process.env.TWITTER_TOKEN_KEY,
            access_token_secret: process.env.TWITTER_TOKEN_SECRET
        });
        // const tweetRegion = await client.post(
        //     'statuses/update', {
        //         status: `${textRegion}${textURL}${process.env.SES_REGION_DATA}`
        //     }
        // )
        // console.info("region tweet", tweetRegion.id, tweetRegion.created_at)

        const tweetCountry = await client.post(
            'statuses/update', {
                status: `${textCountry}${textURL}`
            }
        )
        console.info("country tweet", tweetCountry.id, tweetCountry.created_at)
    } catch (error) {
        console.error('tweet error', error)
    }
}

const orderByData = (a, b, fn) => {
    if (a.data > b.data) return 1
    else if (a.data < b.data) return -1
    else return fn ? fn(a, b) : 0
}
exports.orderByData = orderByData

const getLastValue = (arr) => {
    if (!arr || arr.length == 0) return '2020-02-24T18.00.00'
    return arr[arr.length - 1].data.substring(0, 10)
}

const extendData = (data, isProvince) => {
    const isToReset = (prev, current) => {
        if (!prev) return true
        if (isProvince) {
            return prev.denominazione_regione != current.denominazione_regione ||
                prev.denominazione_provincia != current.denominazione_provincia
        } else {
            return prev.denominazione_regione != current.denominazione_regione
        }
    }
    let prev = null
    for (let index = 0; index < data.length; index++) {
        const curr = data[index]
        Object.assign(
            data[index], {
                totale_casi_ieri: isToReset(prev, curr) ? 0 : data[index - 1].totale_casi,
                totale_nuovi_casi: data[index].totale_casi - (isToReset(prev, curr) ? 0 : data[index - 1].totale_casi),
            })
        if (!isProvince) {
            Object.assign(
                data[index], {
                    dimessi_guariti_ieri: isToReset(prev, curr) ? 0 : data[index - 1].dimessi_guariti,
                    nuovi_dimessi_guariti: data[index].dimessi_guariti - (isToReset(prev, curr) ? 0 : data[index - 1].dimessi_guariti),
                    ricoverati_con_sintomi_ieri: isToReset(prev, curr) ? 0 : data[index - 1].ricoverati_con_sintomi,
                    nuovi_ricoverati_con_sintomi: data[index].ricoverati_con_sintomi - (isToReset(prev, curr) ? 0 : data[index - 1].ricoverati_con_sintomi),
                    deceduti_ieri: isToReset(prev, curr) ? 0 : data[index - 1].deceduti,
                    nuovi_deceduti: data[index].deceduti - (isToReset(prev, curr) ? 0 : data[index - 1].deceduti),
                    terapia_intensiva_ieri: isToReset(prev, curr) ? 0 : data[index - 1].terapia_intensiva,
                    nuovi_terapia_intensiva: data[index].terapia_intensiva - (isToReset(prev, curr) ? 0 : data[index - 1].terapia_intensiva),
                    tamponi_ieri: isToReset(prev, curr) ? 0 : data[index - 1].tamponi,
                    nuovi_tamponi: data[index].tamponi - (isToReset(prev, curr) ? 0 : data[index - 1].tamponi),
                    casi_testati_ieri: isToReset(prev, curr) ? 0 : data[index - 1].casi_testati,
                    nuovi_casi_testati: !isToReset(prev, curr) && data[index - 1].casi_testati == null ?
                        0 : data[index].casi_testati - (isToReset(prev, curr) ?
                            0 : data[index - 1].casi_testati),
                })
        }
        prev = data[index]
    }
    return data;
}

const orderByRegionData = (a, b) => {
    return orderByRegion(a, b, orderByData)
}

const orderByProvinceData = (a, b) => {
    return orderByProvince(a, b, orderByData)
}

const orderByDataRegion = (a, b) => {
    return orderByData(a, b, orderByRegion)
}

const orderByDataProvince = (a, b) => {
    return orderByData(a, b, orderByProvince)
}


const orderByRegion = (a, b, fn) => {
    if (a.denominazione_regione > b.denominazione_regione) return 1
    else if (a.denominazione_regione < b.denominazione_regione) return -1
    else return fn ? fn(a, b) : 0
}

const orderByProvince = (a, b, fn) => {
    if (a.denominazione_regione > b.denominazione_regione) return 1
    else if (a.denominazione_regione < b.denominazione_regione) return -1
    else if (a.denominazione_provincia > b.denominazione_provincia) return 1
    else if (a.denominazione_provincia < b.denominazione_provincia) return -1
    else return fn ? fn(a, b) : 0
}

const filterData = (data, key, value) => {
    const filterSeries = (input) => {
        return input[key].startsWith(value);
    }
    return data.filter(filterSeries)
}

const getDailyRows = (data, day) => {
    const dataValue = day ? day : data[data.length - 1].data
    return filterData(data, 'data', dataValue)
}