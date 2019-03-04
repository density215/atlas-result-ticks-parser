import assert from "assert";
import math from "mathjs";

// We don't use TypedArrays since they are not compatible with mathjs.
// But this seems reasonably fast.

// Given an array `statesArr` of indicator (state) variables, and an array `rttArr` of observations,
// returns an array for each cluster of observtions.
//
// statesArr = [1,1,2,2,1,1]
// rttArr = [1,2,3,4,5,6]

// clusters : [ { id: <STRING>, rtt: [<NUM>,<NUM>,...]}]
const getClusters = (statesArr, rttArr) => {
  assert(statesArr.length === rttArr.length);
  return statesArr.reduce((clustersArr, state, i) => {
    let existState = clustersArr.find(c => c.id === state);
    let newState = {
      id: state,
      rtt: [...((existState && existState.rtt) || []), rttArr[i]]
    };
    if (existState) {
      clustersArr[clustersArr.indexOf(existState)] = newState;
    } else {
      clustersArr.push(newState);
    }
    return clustersArr;
  }, []);
};

function getClusterStats(cluster) {
  let min = math.min(cluster.rtt);
  let max = math.max(cluster.rtt);
  let med = math.median(cluster.rtt);
  // TODO: IQR

  return {
    min: min,
    max: max,
    median: med
  };
}

// Ranges are Python-style (inclusive on the left and exclusive on the right)
// i.e. [a,b).
function getSegments(stateseq, timestamps) {
  assert(stateseq.length == timestamps.length);

  let lastIdx = 0;
  let lastState = stateseq[0];
  let segments = [];

  for (let i = 0; i < stateseq.length; i++) {
    if (stateseq[i] != lastState || i == stateseq.length - 1) {
      segments.push({
        state: lastState,
        start: lastIdx,
        stop: i,
        startTime: timestamps[lastIdx],
        stopTime: timestamps[i]
      });

      lastIdx = i;
      lastState = stateseq[i];
    }
  }

  return segments;
}

export function createSummary({
  stateseq,
  timestamps,
  rtt,
  maxTimeStamp,
  minTimeStamp
}) {
  let segments = getSegments(stateseq, timestamps);
  let clusters = getClusters(stateseq, rtt);
  let statesCount = Object.keys(clusters).length;

  const clustersStats = clusters.map(c => ({
    id: c.id,
    rtt_summary: getClusterStats(c)
  }));

  const clustersStatsWithTotalDuration = clustersStats.map(c => {
    const total_duration = segments
      .filter(seg => seg.state === c.id)
      .reduce(
        (totalDuration, thisSeg) =>
          totalDuration + (thisSeg.stopTime - thisSeg.startTime),
        0
      );
    return {
      ...c,
      summed_duration: total_duration,
      summed_duration_as_percentage_of_total:
        (total_duration / (maxTimeStamp - minTimeStamp)) * 100
    };
  });

  return {
    statesCount: statesCount,
    states: clustersStatsWithTotalDuration.sort(
      (a, b) =>
        a.summed_duration_as_percentage_of_total <
        b.summed_duration_as_percentage_of_total
    ),
    segments: segments
  };
}
