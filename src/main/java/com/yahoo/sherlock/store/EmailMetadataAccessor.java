/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store;

import com.yahoo.sherlock.exception.EmailNotFoundException;
import com.yahoo.sherlock.model.EmailMetaData;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * The {@code EmailMetadataAccessor} defines an interface for
 * communicating with a persistence layer, be it SQL, Redis, etc.,
 * to retrieve and store {@code EmailMetadata} objects.
 */
public interface EmailMetadataAccessor {

    /**
     * Put the given {@code EmailMetadata} object in the store.
     *
     * @param emailMetaData           Object of EmailMetadata class to be stored
     * @throws IOException            if there is an error with the persistence layer
     */
    void putEmailMetadata(EmailMetaData emailMetaData) throws IOException;

    /**
     * Put the new default {@code EmailMetadata} object if the emailId not present.
     *
     * @param emailId                 input emailId
     * @param jobId                   jobId related to the email
     * @throws IOException            if there is an error with the persistence layer
     */
    void putEmailMetadataIfNotExist(String emailId, String jobId) throws IOException;

    /**
     * Get the list of all {@code EmailMetadata} objects.
     *
     * @return the email metadata object list
     * @throws IOException            if there is an error with the persistence layer
     */
    List<EmailMetaData> getAllEmailMetadata() throws IOException;

    /**
     * Get the {@code EmailMetadata} object with the specified emailId.
     *
     * @param emailId the emailId for which to retrieve the email metadata
     * @return the email metadata with the specified emailId
     * @throws IOException            if there is an error with the persistence layer
     * @throws EmailNotFoundException if no email can be found with the specified ID
     */
    EmailMetaData getEmailMetadata(String emailId) throws IOException, EmailNotFoundException;

    /**
     * Get the list of all input emails that are present in instant Email index.
     *
     * @param emails list of input emails
     * @return list of all input emails that are present in instant Email index
     * @throws IOException            if there is an error with the persistence layer
     */
    List<String> checkEmailsInInstantIndex(List<String> emails) throws IOException;

    /**
     * Remove the emailid from the index of given trigger.
     *
     * @param emailId emailid
     * @param trigger trigger name
     * @throws IOException            if there is an error with the persistence layer
     */
    void removeFromTriggerIndex(String emailId, String trigger) throws IOException;

    /**
     * Get the list of all {@code EmailMetaData} objects in a given trigger index.
     *
     * @param trigger trigger name
     * @return the email metadata object list
     * @throws IOException            if there is an error with the persistence layer
     */
    List<EmailMetaData> getAllEmailMetadataByTrigger(String trigger) throws IOException;

    /**
     * Method to delete the email metadata object from all index and database.
     * @param emailMetadata email metadata object to delete
     * @throws IOException io exception
     */
    void deleteEmailMetadata(EmailMetaData emailMetadata) throws IOException;

    /**
     * Method to remove jobId association with given list of emails.
     * @param emailIds list of emails
     * @param jobId jobId
     * @throws IOException io exception
     */
    void removeJobIdFromEmailIndex(List<String> emailIds, String jobId) throws IOException;

    /**
     * Get all email Ids.
     * @return set of email Ids
     */
    Set<String> getAllEmailIds();
}
