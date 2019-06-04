package com.yahoo.sherlock.store.redis;

import com.beust.jcommander.internal.Lists;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import com.yahoo.sherlock.model.AnomalyReport;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.settings.DatabaseConstants;
import com.yahoo.sherlock.store.AnomalyReportAccessor;
import com.yahoo.sherlock.store.StoreParams;
import com.yahoo.sherlock.store.core.AsyncCommands;
import com.yahoo.sherlock.store.core.RedisConnection;
import com.yahoo.sherlock.utils.NumberUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.yahoo.sherlock.store.redis.Mapper.encode;

/**
 * Anomaly report accessor implemented for clusters using Lettuce.
 */
@Slf4j
public class LettuceAnomalyReportAccessor
        extends AbstractLettuceAccessor
        implements AnomalyReportAccessor {

    private final String jobIdName;
    private final String timeName;
    private final String frequencyName;
    private final String emailIdReportName;

    /**
     * @param params store params
     */
    public LettuceAnomalyReportAccessor(StoreParams params) {
        super(params);
        this.jobIdName = params.get(DatabaseConstants.INDEX_REPORT_JOB_ID);
        this.timeName = params.get(DatabaseConstants.INDEX_TIMESTAMP);
        this.frequencyName = params.get(DatabaseConstants.INDEX_FREQUENCY);
        this.emailIdReportName = params.get(DatabaseConstants.INDEX_EMAILID_REPORT);
    }

    /**
     * @param report report to check ID
     * @return whether the report has an assigned ID
     */
    protected static boolean isMissingId(AnomalyReport report) {
        return report.getUniqueId() == null || report.getUniqueId().isEmpty();
    }

    @Override
    public void putAnomalyReports(List<AnomalyReport> reports, List<String> emailIds) throws IOException {
        log.info("Putting [{}] anomaly reports", reports.size());
        try (
                RedisConnection<String> conn = connect();
                RedisConnection<byte[]> binary = binary()
        ) {
            List<AnomalyReport> requireId = new ArrayList<>(reports.size());
            List<AnomalyReport> ready = new ArrayList<>(reports.size());
            AsyncCommands<String> cmd = conn.async();
            AsyncCommands<byte[]> bin = binary.async();
            cmd.setAutoFlushCommands(false);
            bin.setAutoFlushCommands(false);
            for (AnomalyReport report : reports) {
                if (isMissingId(report)) {
                    requireId.add(report);
                } else {
                    ready.add(report);
                }
            }
            if (!requireId.isEmpty()) {
                log.info("Generating new IDs for [{}] reports", requireId.size());
                Integer[] newIds = newIds(requireId.size());
                for (int i = 0; i < newIds.length; i++) {
                    requireId.get(i).setUniqueId(newIds[i].toString());
                }
                ready.addAll(requireId);
                requireId.clear();
            }
            List<RedisFuture> arrFutures = new ArrayList<>(ready.size() + 1);
            RedisFuture[] saddFutures = new RedisFuture[ready.size() * (6 + 2 * emailIds.size())];
            int i = 0;
            long expirationTime = Constants.SECONDS_IN_DAY * (ready.get(0).getJobFrequency().equalsIgnoreCase(Constants.HOUR) ?
                                                              Constants.REDIS_RETENTION_WEEKS_IN_DAYS : (ready.get(0).getJobFrequency().equalsIgnoreCase(Constants.MINUTE) ?
                                                                                                        Constants.REDIS_RETENTION_ONE_DAY : Constants.REDIS_RETENTION_YEARS_IN_DAYS));
            for (AnomalyReport report : ready) {
                arrFutures.addAll(writeReport(bin, cmd, report, expirationTime, this));
                saddFutures[i++] = cmd.sadd(index(jobIdName, report.getJobId()), report.getUniqueId());
                saddFutures[i++] = cmd.expire(index(jobIdName, report.getJobId()), expirationTime);
                saddFutures[i++] = cmd.sadd(index(frequencyName, report.getJobFrequency()), report.getUniqueId());
                saddFutures[i++] = cmd.expire(index(frequencyName, report.getJobFrequency()), expirationTime);
                saddFutures[i++] = cmd.sadd(index(timeName, report.getReportQueryEndTime()), report.getUniqueId());
                saddFutures[i++] = cmd.expire(index(timeName, report.getReportQueryEndTime()), expirationTime);
                for (String emailId : emailIds) {
                    saddFutures[i++] = cmd.sadd(index(emailIdReportName, emailId), report.getUniqueId());
                    saddFutures[i++] = cmd.expire(index(emailIdReportName, emailId), expirationTime);
                }
            }
            arrFutures.addAll(Lists.newArrayList(saddFutures));
            cmd.flushCommands();
            bin.flushCommands();
            awaitRaw(arrFutures);
            log.info("Successfully inserted reports");
        }
    }

    @Override
    public List<AnomalyReport> getAnomalyReportsForJob(String jobId, String frequency) throws IOException {
        log.info("Getting anomaly reports for job [{}] with frequency [{}]", jobId, frequency);
        try (RedisConnection<String> conn = connect()) {
            AsyncCommands<String> cmd = conn.async();
            cmd.setAutoFlushCommands(false);
            RedisFuture<Set<String>> jobReportIds = cmd.smembers(index(jobIdName, jobId));
            RedisFuture<Set<String>> freqReportIds = cmd.smembers(index(frequencyName, frequency));
            cmd.flushCommands();
            await(jobReportIds, freqReportIds);
            Set<String> reportIds = jobReportIds.get();
            reportIds.retainAll(freqReportIds.get());
            return getAnomalyReports(reportIds, this);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error occurred while getting anomaly reports!", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public List<AnomalyReport> getAnomalyReportsForEmailId(String emailId) throws IOException {
        log.info("Getting anomaly reports for Email ID [{}]", emailId);
        try (RedisConnection<String> conn = connect()) {
            AsyncCommands<String> cmd = conn.async();
            cmd.setAutoFlushCommands(false);
            RedisFuture<Set<String>> emailReportIds = cmd.smembers(index(emailIdReportName, emailId));
            cmd.flushCommands();
            await(emailReportIds);
            Set<String> reportIds = emailReportIds.get();
            String[] rarr = new String[reportIds.size()];
            int i = 0;
            for (String s : reportIds) {
                rarr[i] = s; i++;
            }
            if (i > 0) {
                RedisFuture<Long> removedReports = cmd.srem(index(emailIdReportName, emailId), rarr);
                cmd.flushCommands();
                await(removedReports);
            }
            log.info("Removed {} anomaly reports to send it to {}", rarr.length, emailId);
            return getAnomalyReports(reportIds, this);
        } catch (Exception e) {
            log.error("Error occurred while getting anomaly reports!", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public List<AnomalyReport> getAnomalyReportsForJobAtTime(String jobId, String time, String frequency) throws IOException {
        log.info("Getting anomaly reports for job [{}] frequency [{}] at time [{}]", jobId, frequency, time);
        try (RedisConnection<String> conn = connect()) {
            AsyncCommands<String> cmd = conn.async();
            cmd.setAutoFlushCommands(false);
            RedisFuture<Set<String>> jobRepIds = cmd.smembers(index(jobIdName, jobId));
            RedisFuture<Set<String>> jobTimeIds = cmd.smembers(index(timeName, time));
            RedisFuture<Set<String>> jobFreqIds = cmd.smembers(index(frequencyName, frequency));
            cmd.flushCommands();
            await(jobRepIds, jobTimeIds, jobFreqIds);
            Set<String> reportIds = jobRepIds.get();
            reportIds.retainAll(jobTimeIds.get());
            reportIds.retainAll(jobFreqIds.get());
            return getAnomalyReports(reportIds, this);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error occurred while getting anomaly reports!", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public void deleteAnomalyReportsForJob(String jobId) throws IOException {
        log.info("Deleting all anomaly reports for job [{}]", jobId);
        try (
                RedisConnection<String> conn = connect();
                RedisConnection<byte[]> binary = binary()
        ) {
            AsyncCommands<String> cmd = conn.async();
            AsyncCommands<byte[]> bin = binary.async();
            Set<String> reportIds = cmd.smembers(index(jobIdName, jobId)).get();
            cmd.setAutoFlushCommands(false);
            bin.setAutoFlushCommands(false);
            List<AnomalyReport> reports = getAnomalyReports(reportIds, this);
            RedisFuture[] futures = new RedisFuture[5 * reports.size() + 1];
            int i = 0;
            for (AnomalyReport report : reports) {
                futures[i++] = cmd.srem(index(timeName, report.getReportQueryEndTime()), report.getUniqueId());
                futures[i++] = cmd.srem(index(frequencyName, report.getJobFrequency()), report.getUniqueId());
                futures[i++] = cmd.del(key(report.getUniqueId()));
                futures[i++] = bin.del(encode(key(report.getUniqueId(), DatabaseConstants.ANOMALY_TIMESTAMP, "start")));
                futures[i++] = bin.del(encode(key(report.getUniqueId(), DatabaseConstants.ANOMALY_TIMESTAMP, "end")));
            }
            futures[i] = cmd.del(index(jobIdName, jobId));
            cmd.flushCommands();
            bin.flushCommands();
            await(futures);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error while deleting anomaly reports!", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public void deleteAnomalyReportsForJobAtTime(String jobId, String time, String frequency) throws IOException {
        log.info("Getting anomaly reports for job [{}] frequency [{}] at time [{}] for deletion", jobId, frequency, time);
        try (
            RedisConnection<String> conn = connect();
            RedisConnection<byte[]> binary = binary()
        ) {
            // Get all report IDs
            AsyncCommands<String> cmd = conn.async();
            AsyncCommands<byte[]> bin = binary.async();
            cmd.setAutoFlushCommands(false);
            RedisFuture<Set<String>> jobRepIds = cmd.smembers(index(jobIdName, jobId));
            RedisFuture<Set<String>> jobTimeIds = cmd.smembers(index(timeName, time));
            RedisFuture<Set<String>> jobFreqIds = cmd.smembers(index(frequencyName, frequency));
            cmd.flushCommands();
            await(jobRepIds, jobTimeIds, jobFreqIds);
            Set<String> reportIds = jobRepIds.get();
            reportIds.retainAll(jobTimeIds.get());
            reportIds.retainAll(jobFreqIds.get());
            // Delete the reports
            cmd.setAutoFlushCommands(false);
            bin.setAutoFlushCommands(false);
            List<AnomalyReport> reports = getAnomalyReports(reportIds, this);
            RedisFuture[] futures = new RedisFuture[6 * reports.size()];
            int i = 0;
            for (AnomalyReport report : reports) {
                futures[i++] = cmd.srem(index(jobIdName, report.getJobId()), report.getUniqueId());
                futures[i++] = cmd.srem(index(timeName, report.getReportQueryEndTime()), report.getUniqueId());
                futures[i++] = cmd.srem(index(frequencyName, report.getJobFrequency()), report.getUniqueId());
                futures[i++] = cmd.del(key(report.getUniqueId()));
                futures[i++] = bin.del(encode(key(report.getUniqueId(), DatabaseConstants.ANOMALY_TIMESTAMP, "start")));
                futures[i++] = bin.del(encode(key(report.getUniqueId(), DatabaseConstants.ANOMALY_TIMESTAMP, "end")));
            }
            cmd.flushCommands();
            bin.flushCommands();
            await(futures);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error occurred while deleting anomaly reports!", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    /**
     * Write a report to the store, exacting the timestamps and
     * encoding them as bytes.
     *
     * @param bin    binary commands
     * @param cmd    string commands
     * @param report report to write
     * @param expirationTime expiration time of the key
     * @param acc    accessor instance
     * @return an array of futures that need to be awaited
     */
    @SuppressWarnings("unchecked")
    protected static List<RedisFuture> writeReport(
            AsyncCommands<byte[]> bin,
            AsyncCommands<String> cmd,
            AnomalyReport report,
            long expirationTime,
            AbstractLettuceAccessor acc
    ) {
        List<int[]> timestamps = report.getAnomalyTimestampsHours();
        Map<String, String> reportMap = acc.map(report);
        reportMap.remove(DatabaseConstants.ANOMALY_TIMESTAMP);
        byte[] keyStart = encode(acc.key(report.getUniqueId(), DatabaseConstants.ANOMALY_TIMESTAMP, "start"));
        byte[] keyEnd = encode(acc.key(report.getUniqueId(), DatabaseConstants.ANOMALY_TIMESTAMP, "end"));
        List<ScoredValue<byte[]>> valuesStart = new ArrayList<>(timestamps.size());
        List<ScoredValue<byte[]>> valuesEnd = new ArrayList<>(timestamps.size());
        for (int i = 0; i < timestamps.size(); i++) {
            int[] timestamp = timestamps.get(i);
            valuesStart.add(ScoredValue.fromNullable((double) i, NumberUtils.toBytesCompressed(timestamp[0])));
            if (timestamp[1] != 0 && timestamp[1] != timestamp[0]) {
                valuesEnd.add(ScoredValue.fromNullable((double) i, NumberUtils.toBytesCompressed(timestamp[1])));
            }
        }
        List<RedisFuture> futures = new ArrayList<>(3);
        futures.add(cmd.hmset(acc.key(report.getUniqueId()), reportMap));
        futures.add(cmd.expire(acc.key(report.getUniqueId()), expirationTime));
        log.info("Report " + report.getUniqueId() + " will expire in " + expirationTime / Constants.SECONDS_IN_DAY + " days");
        if (!valuesStart.isEmpty()) {
            futures.add(bin.zadd(keyStart, valuesStart.toArray(new ScoredValue[valuesStart.size()])));
            futures.add(bin.expire(keyStart, expirationTime));
        }
        if (!valuesEnd.isEmpty()) {
            futures.add(bin.zadd(keyEnd, valuesEnd.toArray(new ScoredValue[valuesEnd.size()])));
            futures.add(bin.expire(keyEnd, expirationTime));
        }
        return futures;
    }

    /**
     * Get a list of anomaly reports corresponding
     * to a set of report IDs.
     *
     * @param reportIds set of report IDs
     * @param acc       accessor instance
     * @return list of anomaly reports
     * @throws IOException if an error occurs
     */
    protected static List<AnomalyReport> getAnomalyReports(
            Set<String> reportIds,
            AbstractLettuceAccessor acc
    ) throws IOException {
        try (
                RedisConnection<String> conn = acc.connect();
                RedisConnection<byte[]> binary = acc.binary()
        ) {
            List<RedisFuture<Map<String, String>>> values = new ArrayList<>(reportIds.size());
            List<RedisFuture<List<ScoredValue<byte[]>>>> timeStart = new ArrayList<>(reportIds.size());
            List<RedisFuture<List<ScoredValue<byte[]>>>> timeEnd = new ArrayList<>(reportIds.size());
            AsyncCommands<String> cmd = conn.async();
            AsyncCommands<byte[]> bin = binary.async();
            cmd.setAutoFlushCommands(false);
            bin.setAutoFlushCommands(false);
            for (String id : reportIds) {
                byte[] keyStart = encode(acc.key(id, DatabaseConstants.ANOMALY_TIMESTAMP, "start"));
                byte[] keyEnd = encode(acc.key(id, DatabaseConstants.ANOMALY_TIMESTAMP, "end"));
                values.add(cmd.hgetall(acc.key(id)));
                timeStart.add(bin.zrangeWithScores(keyStart, 0, -1));
                timeEnd.add(bin.zrangeWithScores(keyEnd, 0, -1));
            }
            cmd.flushCommands();
            bin.flushCommands();
            List<RedisFuture[]> combine = Lists.newArrayList(
                    values.toArray(new RedisFuture[values.size()]),
                    timeStart.toArray(new RedisFuture[timeStart.size()]),
                    timeEnd.toArray(new RedisFuture[timeEnd.size()])
            );
            acc.awaitCollection(combine);
            List<AnomalyReport> reports = new ArrayList<>(values.size());
            for (int i = 0; i < values.size(); i++) {
                AnomalyReport report = acc.unmap(AnomalyReport.class, values.get(i).get());
                decodeAndSetTimestamp(report, timeStart.get(i).get(), timeEnd.get(i).get());
                reports.add(report);
            }
            return reports;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error while getting anomaly reports!", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    /**
     * Decode timestamps from lettuce responses.
     *
     * @param report    report to set timestamps
     * @param startVals starting scored values
     * @param endVals   ending scored values
     */
    protected static void decodeAndSetTimestamp(
            AnomalyReport report,
            List<ScoredValue<byte[]>> startVals,
            List<ScoredValue<byte[]>> endVals
    ) {
        byte[][] startBytes = new byte[startVals.size()][];
        byte[][] endBytes = new byte[startVals.size()][];
        for (ScoredValue<byte[]> val : startVals) {
            startBytes[(int) val.getScore()] = val.getValue();
        }
        for (ScoredValue<byte[]> val : endVals) {
            endBytes[(int) val.getScore()] = val.getValue();
        }
        report.setAnomalyTimestampsFromBytes(startBytes, endBytes);
    }

}
