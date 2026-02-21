package com.jpmc.midascore.entity;

import jakarta.persistence.*;

@Entity
public class TransactionRecord {

    @Id
    @GeneratedValue
    private long id;

    @ManyToOne
    private UserRecord senderId;

    @ManyToOne
    private UserRecord recipientId;

    @Column(nullable = false)
    private float amount;

    @Column(nullable = false)
    private float incentive;

    public TransactionRecord(UserRecord senderId,UserRecord recipientId, float amount,float incentive){
        this.senderId = senderId;
        this.recipientId = recipientId;
        this.amount = amount;
        this.incentive = incentive;
    }

    public UserRecord getSenderId(){return this.senderId;}
    public UserRecord getRecipientId(){return this.recipientId;}
    public float getAmount(){return this.amount;}
    public float getIncentive(){return this.incentive;}
}
