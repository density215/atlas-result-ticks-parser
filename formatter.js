const assert = require('assert');
const math = require('mathjs');

// We don't use TypedArrays since they are not compatible with mathjs.
// But this seems reasonably fast.

// Given an array `x` of indicator (state) variables, and an array `y` of observations,
// returns an array for each cluster of observtions.
//
// x = [1,1,2,2,1,1]
// y = [1,2,3,4,5,6]
// getClusters(x,y) => {'1': [1,2,5,6], '2': [3,4]}
function getClusters(x, y) {
    assert(x.length == y.length);
    clusters = {};

    for (i = 0; i < x.length; i++) {
        state = x[i];
        obs = y[i];

        if (!(state in clusters)) {
            clusters[state] = [obs];
        } else {
            clusters[state].push(obs);
        }
    };

    return clusters;
}

function getClusterStats(cluster) {
    min = math.min(cluster);
    max = math.max(cluster);
    med = math.median(cluster);
    // TODO: IQR
    // TODO: Duration

    return {
        'rtt': {
            'min': min,
            'max': max,
            'median': med
        }
    }
}

// Ranges are Python-style (inclusive on the left and exclusive on the right)
// i.e. [a,b).
function getSegments(stateseq, timestamps) {
    assert(stateseq.length == timestamps.length);

    lastIdx = 0;
    lastState = stateseq[0];
    segments = [];

    for (i = 0; i < stateseq.length; i++) {
        if ((stateseq[i] != lastState) || (i == stateseq.length - 1)) {
            segments.push({
                'state': lastState,
                'start': lastIdx,
                'stop': i,
                'startTime': timestamps[lastIdx],
                'stopTime': timestamps[i]
            });

            lastIdx = i;
            lastState = stateseq[i];
        }
    }

    return segments;
}

function getOutput(stateseq, timestamps, rtt) {
    segments = getSegments(stateseq, timestamps);
    clusters = getClusters(stateseq, rtt);
    statesCount = Object.keys(clusters).length;

    clustersStats = {};
    for (key of Object.keys(clusters)) {
        clustersStats[key] = getClusterStats(clusters[key]);
    }

    return {
        'statesCount': statesCount,
        'states': clustersStats,
        'segments': segments
    }
}

exports.getOutput = getOutput;

// const formatter = require('./formatter');
// console.log(formatter.getOutput(res, timestamp, rtt));
