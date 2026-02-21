package com.jpmc.midascore.component;

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
    private long waldorfId = -1;

    public TransactionListener(UserRepository userRepository, TransactionRecordRepository transactionRecordRepository){
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
    }

    public boolean findUser(long senderId) {
        if (userRepository.existsById(senderId)) return true;
        else return false;
    }

    private UserRecord getUserById(long Id){
        UserRecord user = userRepository.findById(Id);
        if(user.getName().equals("waldorf")) {
            waldorfId = user.getId();
//            System.out.println("Here");
//            System.out.println(waldorfId);
        }


        return user;
    }

    public boolean checkSendersBalance(long senderId, float amount){
        UserRecord user = this.getUserById(senderId);
        if(user.getBalance() >= amount) return true;
        else return false;
    }
    public void updateUserAmount(long userId, float amount){
        UserRecord user = this.getUserById(userId);
        user.setBalance(user.getBalance() + amount);
        userRepository.save(user);
        return;
    }

    @KafkaListener(id="listerner", topics = "${general.kafka-topic}")
    public void listener(Transaction transaction){
        System.out.println("Message Recieved");
        if(!findUser(transaction.getSenderId())) return;
        if(!findUser(transaction.getRecipientId())) return;
        if(checkSendersBalance(transaction.getSenderId(),transaction.getAmount())) {
            TransactionRecord transactionRecord = new TransactionRecord(this.getUserById(transaction.getSenderId()), this.getUserById(transaction.getRecipientId()), transaction.getAmount());
            transactionRecordRepository.save(transactionRecord);
            updateUserAmount(transaction.getRecipientId(), transaction.getAmount());
            updateUserAmount(transaction.getSenderId(), -1 * transaction.getAmount());
        }
        if(waldorfId!=-1){
            System.out.println(getUserById(waldorfId).getBalance());
        }

        return;

    }
}
