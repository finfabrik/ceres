package com.blokaly.ceres.hitbtc.data;

import com.blokaly.ceres.common.DecimalNumber;
import com.blokaly.ceres.data.OrderInfo;

public class OrderRecord implements OrderInfo {
    private DecimalNumber price;
    private DecimalNumber quantity;
    private Side side;

    OrderRecord(DecimalNumber price, DecimalNumber quantity, Side side){
        this.price = price;
        this.quantity = quantity;
        this.side = side;
    }


    public DecimalNumber getPrice() {
        return price;
    }

    public DecimalNumber getQuantity(){
        return quantity;
    }

    @Override
    public Side side() {
        return side;
    }

    @Override
    public String toString() {
        return "{price=" + price +
                ", quantity=" + quantity +
                ", side=" + side + "}";
    }
}

