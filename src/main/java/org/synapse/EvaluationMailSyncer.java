package org.synapse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.PaginatedResults;
import org.sagebionetworks.repo.model.UserProfile;

import com.ecwid.mailchimp.MailChimpClient;
import com.ecwid.mailchimp.MailChimpException;
import com.ecwid.mailchimp.MailChimpObject;
import com.ecwid.mailchimp.method.v1_3.list.ListBatchSubscribeMethod;
import com.ecwid.mailchimp.method.v1_3.list.ListMembersMethod;
import com.ecwid.mailchimp.method.v1_3.list.ListMembersResult;
import com.ecwid.mailchimp.method.v1_3.list.MemberStatus;
import com.ecwid.mailchimp.method.v1_3.list.ShortMemberInfo;

/**
 * The worker that processes messages for Evaluation asynchronous jobs.
 * 
 * @author dburdick
 *
 */
public class EvaluationMailSyncer {
	
	static private Log log = LogFactory.getLog(EvaluationMailSyncer.class);
	static private final String OVERALL_DREAM_MAILCHIMP_LIST_ID = "6abd850dde";
	
	String mailChimpApiKey;
	MailChimpClient mailChimpClient;
	SynapseClient synapse;
	
	public EvaluationMailSyncer(String mailChimpApiKey, String synapseUsername,
			String synapsePassword) throws SynapseException {
		if(mailChimpApiKey == null) throw new IllegalArgumentException("mailChimpApiKey cannot be null");
		if(synapseUsername == null) throw new IllegalArgumentException("synapseUserName cannot be null");
		if(synapsePassword == null) throw new IllegalArgumentException("synapsePassword cannot be null");
		
		this.mailChimpApiKey = mailChimpApiKey;
		this.mailChimpClient = new MailChimpClient();
		this.synapse = createSynapseClient();
		synapse.login(synapseUsername, synapsePassword);
	}

	public void sync() throws Exception {
		//adds all Synapse users to overall dream mailchimp master list (if not already there)
		int added = 0;
		Set<String> listEmails = getAllListEmails(OVERALL_DREAM_MAILCHIMP_LIST_ID);				
		
		long total = Integer.MAX_VALUE; // starting value
		int offset = 0;
		int limit = 250;
		while(offset < total) {
			int toAdd = 0;
			PaginatedResults<UserProfile> batch = synapse.getUsers(offset, limit);
			total = batch.getTotalNumberOfResults();
			List<MailChimpObject> mcBatch = new ArrayList<MailChimpObject>();
			List<UserProfile> userProfiles = batch.getResults();
			for(UserProfile userProfile : userProfiles) {
				try {
					// get user's email and if not in email list already, add
					String participantEmail = userProfile.getEmails().get(0);
					if(participantEmail == null && userProfile.getEmails() != null && userProfile.getEmails().size() > 0)
						participantEmail = userProfile.getEmails().get(0);

					if(participantEmail != null && !listEmails.contains(participantEmail)) {
						MailChimpObject obj = new MailChimpObject();
						obj.put("EMAIL", participantEmail);					
						obj.put("EMAIL_TYPE", "html");
						obj.put("FNAME", userProfile.getFirstName());
						obj.put("LNAME", userProfile.getLastName());
						mcBatch.add(obj);
						toAdd++;
					}
				} catch (Exception e) {
					log.error("Error retrieving user email: "+ userProfile.getOwnerId(), e);
				}
			}
			if (toAdd > 0) {
				// add to list AND the overall Dream list
				ListBatchSubscribeMethod subscribeRequest = new ListBatchSubscribeMethod();
				subscribeRequest.apikey = mailChimpApiKey;
				subscribeRequest.id = OVERALL_DREAM_MAILCHIMP_LIST_ID;
				subscribeRequest.double_optin = false;
				subscribeRequest.update_existing = false;
				subscribeRequest.batch = mcBatch;
				
				mailChimpClient.execute(subscribeRequest);
			}
			added += toAdd;
			offset += limit;
		}
		log.error("New emails added: " + added);
	}

	private Set<String> getAllListEmails(String listId) throws IOException, MailChimpException {
		Set<String> emails = new HashSet<String>();
		// get all subscribed & unsubscribed members of the list
		for(MemberStatus status : new MemberStatus[]{ MemberStatus.subscribed, MemberStatus.unsubscribed}) {
			int offset = 0;
			int limit = 14999;
			boolean done = false;	
			while(!done) {
				ListMembersMethod request = new ListMembersMethod();
				request.apikey = mailChimpApiKey;
				request.id = listId;
				request.status = status;
				request.start = offset;
				request.limit = limit;
				
				ListMembersResult result = mailChimpClient.execute(request);
				for(ShortMemberInfo member : result.data) {
					if(member.email != null && !member.email.isEmpty())
						emails.add(member.email);
				}
				
				offset += limit;			
				if(result.total <= offset) done = true;
			}
		}
		return emails;
	}

	private SynapseClient createSynapseClient() {
		SynapseClient synapseClient = new SynapseClientImpl();
//		synapseClient.setRepositoryEndpoint(StackConfiguration.getRepositoryServiceEndpoint());
//		synapseClient.setAuthEndpoint(StackConfiguration.getAuthenticationServicePublicEndpoint());
		return synapseClient;
	}

}
