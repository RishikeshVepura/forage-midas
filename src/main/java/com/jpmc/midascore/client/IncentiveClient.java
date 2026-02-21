package com.jpmc.midascore.client;

import com.jpmc.midascore.dto.IncentiveResponse;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class IncentiveClient {
    private final RestTemplate restTemplate;
    @Value("${external.incentive.url}")
    private String url;
    public IncentiveClient(RestTemplate restTemplate){
        this.restTemplate = restTemplate;
    }

    public IncentiveResponse fetchIncentiveResponse(Transaction transaction){
        return restTemplate.postForObject(
                url,
                transaction,        // Spring auto-serializes this
                IncentiveResponse.class     // Spring auto-deserializes this
        );
    }
}
