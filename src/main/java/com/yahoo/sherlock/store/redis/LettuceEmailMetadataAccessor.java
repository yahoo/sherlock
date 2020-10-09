/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store.redis;

import io.lettuce.core.RedisFuture;
import com.yahoo.sherlock.enums.Triggers;
import com.yahoo.sherlock.exception.EmailNotFoundException;
import com.yahoo.sherlock.model.EmailMetaData;
import com.yahoo.sherlock.settings.DatabaseConstants;
import com.yahoo.sherlock.store.EmailMetadataAccessor;
import com.yahoo.sherlock.store.StoreParams;
import com.yahoo.sherlock.store.core.AsyncCommands;
import com.yahoo.sherlock.store.core.RedisConnection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import lombok.extern.slf4j.Slf4j;

/**
 * Email metadata accessor implemented for clusters with lettuce.
 */
@Slf4j
public class LettuceEmailMetadataAccessor extends AbstractLettuceAccessor implements EmailMetadataAccessor {

    private final String emailIdIndex;
    private final String emailTriggerIndex;
    private final String emailJobIndex;

    /**
     * @param params store parameters
     */
    public LettuceEmailMetadataAccessor(StoreParams params) {
        super(params);
        this.emailIdIndex = params.get(DatabaseConstants.INDEX_EMAIL_ID);
        this.emailTriggerIndex = params.get(DatabaseConstants.INDEX_EMAILID_TRIGGER);
        this.emailJobIndex = params.get(DatabaseConstants.INDEX_EMAILID_JOBID);
    }

    @Override
    public void putEmailMetadata(EmailMetaData emailMetaData) throws IOException {
        log.info("Putting Email metadata with ID [{}]", emailMetaData.getEmailId());
        String emailId = emailMetaData.getEmailId();
        try (RedisConnection<String> conn = connect()) {
            if (emailId != null && !emailId.isEmpty()) {
                AsyncCommands<String> cmd = conn.async();
                cmd.setAutoFlushCommands(false);
                RedisFuture[] futures = {
                    cmd.hmset(key(emailMetaData.getEmailId()), map(emailMetaData)),
                    cmd.sadd(index(emailIdIndex, DatabaseConstants.EMAILS), emailId)
                };
                cmd.flushCommands();
                await(futures);
                RedisFuture<Long> futureIndex;
                String interval = emailMetaData.getRepeatInterval();
                futureIndex = cmd.sadd(index(emailTriggerIndex, interval), emailId);
                cmd.flushCommands();
                await(futureIndex);
                log.info("Added emailId:{} to {} index", emailId, interval);
                log.info("Successfully added new Email ID [{}]", emailId);
            } else {
                log.error("Email ID can not be null or empty! value: {}!", emailId);
            }
        }
    }

    @Override
    public void putEmailMetadataIfNotExist(String emailId, String jobId) throws IOException {
        log.info("Putting Email with ID [{}] if not already exist", emailId);
        try (RedisConnection<String> conn = connect()) {
            if (emailId != null) {
                AsyncCommands<String> cmd = conn.async();
                cmd.setAutoFlushCommands(false);
                RedisFuture<Long> checkIndex = cmd.sadd(index(emailIdIndex, DatabaseConstants.EMAILS), emailId);
                RedisFuture<Long> jobEmailIndex = cmd.sadd(index(emailJobIndex, emailId), jobId);
                cmd.flushCommands();
                await(checkIndex, jobEmailIndex);
                if (checkIndex.get() != 0L) {
                    putEmailMetadata(new EmailMetaData(emailId));
                } else {
                    log.info("[{}] already exist", emailId);
                }
            } else {
                log.error("Email ID can not be null!");
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error occurred while deleting job!", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public List<EmailMetaData> getAllEmailMetadata() throws IOException {
        log.info("Getting metadata for all emails");
        try (RedisConnection<String> conn = connect()) {
            AsyncCommands<String> cmd = conn.async();
            cmd.setAutoFlushCommands(false);
            RedisFuture<Set<String>> emailFutureList = cmd.smembers(index(emailIdIndex, DatabaseConstants.EMAILS));
            cmd.flushCommands();
            await(emailFutureList);
            Set<String> emailList = emailFutureList.get();
            List<RedisFuture<Map<String, String>>> values = new ArrayList<>(emailList.size());
            log.info("Fetching metadata for {} emails", emailList.size());
            for (String emailId : emailList) {
                values.add(cmd.hgetall(key(emailId)));
            }
            cmd.flushCommands();
            await(values);
            List<EmailMetaData> emailMetaDataList = new ArrayList<>(values.size());
            for (RedisFuture<Map<String, String>> value : values) {
                emailMetaDataList.add(unmap(EmailMetaData.class, value.get()));
            }
            return emailMetaDataList;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error occurred while getting emails metadata!", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public EmailMetaData getEmailMetadata(String emailId) throws IOException, EmailNotFoundException {
        log.info("Getting metadata for email {}", emailId);
        try (RedisConnection<String> conn = connect()) {
            AsyncCommands<String> cmd = conn.async();
            cmd.setAutoFlushCommands(false);
            RedisFuture<Map<String, String>> value = cmd.hgetall(key(emailId));
            cmd.flushCommands();
            await(value);
            if (value.get() != null && value.get().isEmpty()) {
                throw new EmailNotFoundException(emailId);
            }
            return unmap(EmailMetaData.class, value.get());
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error occurred while getting emails metadata!", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public List<String> checkEmailsInInstantIndex(List<String> emails) throws IOException {
        log.info("Checking for emails in Instant index");
        try (RedisConnection<String> conn = connect()) {
            AsyncCommands<String> cmd = conn.async();
            cmd.setAutoFlushCommands(false);
            RedisFuture<Set<String>> future = cmd.smembers(index(emailTriggerIndex, Triggers.INSTANT.toString()));
            cmd.flushCommands();
            await(future);
            List<String> result = new ArrayList<>();
            Set<String> instantList = future.get();
            for (String email : emails) {
                if (instantList.contains(email)) {
                    result.add(email);
                }
            }
            return result;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error occurred while getting instant email index!", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public void removeFromTriggerIndex(String emailId, String trigger) throws IOException {
        log.info("Removing {} from email trigger {} index", emailId, trigger);
        try (RedisConnection<String> conn = connect()) {
            AsyncCommands<String> cmd = conn.async();
            cmd.setAutoFlushCommands(false);
            RedisFuture<Long> future = cmd.srem(index(emailTriggerIndex, trigger), emailId);
            cmd.flushCommands();
            await(future);
            log.info("Removed {} from index. redis response: {}", emailId, future.get().toString());
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error occurred while getting instant email index!", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public List<EmailMetaData> getAllEmailMetadataByTrigger(String trigger) throws IOException {
        log.info("Getting metadata for all emails for trigger {}", trigger);
        try (RedisConnection<String> conn = connect()) {
            AsyncCommands<String> cmd = conn.async();
            cmd.setAutoFlushCommands(false);
            RedisFuture<Set<String>> emailFutureList = cmd.smembers(index(emailTriggerIndex, trigger));
            cmd.flushCommands();
            await(emailFutureList);
            Set<String> emailList = emailFutureList.get();
            List<RedisFuture<Map<String, String>>> values = new ArrayList<>(emailList.size());
            log.info("Fetching metadata for {} emails", emailList.size());
            for (String emailId : emailList) {
                values.add(cmd.hgetall(key(emailId)));
            }
            cmd.flushCommands();
            await(values);
            List<EmailMetaData> emailMetaDataList = new ArrayList<>(values.size());
            for (RedisFuture<Map<String, String>> value : values) {
                emailMetaDataList.add(unmap(EmailMetaData.class, value.get()));
            }
            return emailMetaDataList;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error occurred while getting emails metadata!", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public void deleteEmailMetadata(EmailMetaData emailMetadata) throws IOException {
        String emailId = emailMetadata.getEmailId();
        String trigger = emailMetadata.getRepeatInterval();
        log.info("Deleting email {}", emailId);
        try (RedisConnection<String> conn = connect()) {
            AsyncCommands<String> cmd = conn.async();
            cmd.setAutoFlushCommands(false);
            RedisFuture[] emailIndexFutureList = new RedisFuture[] {
                cmd.srem(index(emailTriggerIndex, trigger), emailId),
                cmd.srem(index(emailIdIndex, DatabaseConstants.EMAILS), emailId),
                cmd.del(index(DatabaseConstants.INDEX_EMAILID_REPORT, emailId)),
                cmd.del(key(emailId))
            };
            cmd.flushCommands();
            await(emailIndexFutureList);
            log.info("Succesfully deleted email {}", emailId);
        } catch (Exception e) {
            log.error("Error occurred while deleting emails metadata!", e);
            throw new IOException(e.getMessage(), e);
        }
    }
}
