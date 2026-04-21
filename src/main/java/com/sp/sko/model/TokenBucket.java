package com.sp.sko.model;

public class TokenBucket {
    private final long capacity;
    private double availableTokens;
    private long lastRefillMillis;
    private final double refillTokenPerMillis;

    public TokenBucket(long capacity, long refillTokens, long refillPeriodMillis){
        this.capacity = capacity;
        availableTokens = capacity;
        lastRefillMillis = System.currentTimeMillis();
        refillTokenPerMillis = (double) refillTokens / (double) refillPeriodMillis;
    }

    synchronized public boolean tryConsume(int numberTokens){
        addTokens();
        if(availableTokens > numberTokens){
            availableTokens -= numberTokens;
            return true;
        }else{
            return false;
        }
    }

    public void addTokens(){
        long systemCurrentMillis = System.currentTimeMillis();
        long sinceLastRefillMillis = systemCurrentMillis - lastRefillMillis;
        double refill = sinceLastRefillMillis * refillTokenPerMillis;
        availableTokens = Math.min(capacity, availableTokens + refill);
        lastRefillMillis = systemCurrentMillis;
    }
}
