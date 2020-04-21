const { Octokit } = require('@octokit/rest'),
    moment = require('moment'),
    path = require('path')

const GITHUB_REF = 'heads/master',
    GITHUB_TOKEN = process.env.GITHUB_TOKEN,
    GITHUB_REPO = process.env.GITHUB_REPO,
    GITHUB_USER = process.env.GITHUB_USER;

const octokit = new Octokit({
    auth: 'token ' + GITHUB_TOKEN
});

exports.commitPushFiles = async(dataFiles, message = `${moment().format()}`) => {
    if (!GITHUB_USER || !GITHUB_REPO || !GITHUB_TOKEN) {
        console.log('missing env GITHUB_USER,  GITHUB_REPO or GITHUB_TOKEN)')
        process.exit(-1);
    }
    const outBlobs = [];
    for (const dataFile of dataFiles) {
        console.log('commit ', dataFile.path);
        var content = Buffer.from(
            path.extname(dataFile.path) == '.json' ?
            JSON.stringify(dataFile.content, null, 4) :
            dataFile.content).toString("base64")
        const outBlob = await octokit.git.createBlob({
            owner: GITHUB_USER,
            repo: GITHUB_REPO,
            content: content,
            encoding: 'base64'
        });
        //console.log('blob sha', outBlob.data.sha);
        outBlobs.push({
            path: dataFile.path,
            mode: '100644',
            type: 'blob',
            sha: outBlob.data.sha
        });
    }
    const reference = await octokit.git.getRef({
        owner: GITHUB_USER,
        repo: GITHUB_REPO,
        ref: GITHUB_REF
    });
    //console.log('reference (last commit) sha', reference.data.object.sha);
    const lastCommitSha = reference.data.object.sha;
    const lastTree = await octokit.git.getTree({
        owner: GITHUB_USER,
        repo: GITHUB_REPO,
        tree_sha: lastCommitSha
    });
    //console.log('tree sha', lastTree.data.sha);
    const outTree = await octokit.git.createTree({
        owner: GITHUB_USER,
        repo: GITHUB_REPO,
        tree: outBlobs,
        base_tree: lastTree.data.sha
    });
    //console.log('tree sha', outTree.data.sha);
    const outCommit = await octokit.git.createCommit({
        owner: GITHUB_USER,
        repo: GITHUB_REPO,
        message: message,
        tree: outTree.data.sha,
        parents: [
            lastCommitSha
        ]
    });
    //console.log('out commit sha', outCommit.data.sha);
    await octokit.git.updateRef({
        owner: GITHUB_USER,
        repo: GITHUB_REPO,
        ref: GITHUB_REF,
        sha: outCommit.data.sha,
        force: true
    });
    console.log('push done');
}

/**
 * check if body's HMAC sha1 signature generated is equal to X-Hub-Signature's request header
 * @param {string} hubSignatureSent the Hub Signature sent in format 'sha1=XXXX'
 * @param {string} secretKey the secret key used to sign
 * @param {Buffer} rawBody the Body raw buffer
 */
exports.checkHubSignature = (xHubSignatureHeader, secretKey, rawBody) => {
    var hmac = crypto.createHmac("sha1", secretKey);
    hmac.update(rawBody, "utf-8");
    return ("sha1=" + hmac.digest("hex")) == xHubSignatureHeader;
};