package com.sp.sko.model;

public record OrderEvent (String orderId, String userId, double amount) {
}
