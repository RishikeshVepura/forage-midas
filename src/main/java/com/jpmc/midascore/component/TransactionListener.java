package com.jpmc.midascore.component;

import com.jpmc.midascore.client.IncentiveClient;
import com.jpmc.midascore.dto.IncentiveResponse;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {
    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final IncentiveClient incentiveService;

    public TransactionListener(UserRepository userRepository, TransactionRecordRepository transactionRecordRepository,IncentiveClient incentiveClient){
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
        this.incentiveService = incentiveClient;
    }

    public boolean findUser(long senderId) {
        return userRepository.existsById(senderId);
    }

    private UserRecord getUserById(long Id){
        return userRepository.findById(Id);
    }

    public boolean checkSendersBalance(long senderId, float amount){
        UserRecord user = this.getUserById(senderId);
        return user.getBalance() >= amount;
    }
    public void updateUserAmount(long userId, float amount){
        UserRecord user = this.getUserById(userId);
        user.setBalance(user.getBalance() + amount);
        userRepository.save(user);
    }

    @KafkaListener(id="listerner", topics = "${general.kafka-topic}")
    public void listener(Transaction transaction){
        System.out.println("Message Recieved");
        if(!findUser(transaction.getSenderId())) return;
        if(!findUser(transaction.getRecipientId())) return;
        if(checkSendersBalance(transaction.getSenderId(),transaction.getAmount())) {
            IncentiveResponse incentive = this.incentiveService.fetchIncentiveResponse(transaction);
            updateUserAmount(transaction.getRecipientId(), incentive.amount());
            TransactionRecord transactionRecord = new TransactionRecord(this.getUserById(transaction.getSenderId()), this.getUserById(transaction.getRecipientId()), transaction.getAmount(), incentive.amount());
            transactionRecordRepository.save(transactionRecord);
            updateUserAmount(transaction.getRecipientId(), transaction.getAmount());
            updateUserAmount(transaction.getSenderId(), -1 * transaction.getAmount());

        }
    }
}
