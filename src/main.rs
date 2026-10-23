use orderbook_rs::orderbook::OrderBook;
use orderbook_rs::types::{Order, OrderType, Side, TimeInForce};

fn main() {
    let mut book = OrderBook::new();

    let order1 = Order {
        id: 1,
        side: Side::Ask,
        price: 100,
        quantity: 5,
        timestamp: 1,
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::GTC,
    };
    book.submit(order1);

    let taker = Order {
        id: 6,
        side: Side::Bid,
        price: 105,
        quantity: 10,
        timestamp: 2,
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::GTC,
    };

    let market_order_1 = Order {
        id: 7,
        side: Side::Ask,
        price: 0, // price is ignored for market orders
        quantity: 2,
        timestamp: 3,
        order_type: OrderType::Market,
        time_in_force: TimeInForce::IOC,
    };

    let remaining = book.submit(taker).remaining_quantity;

    let remaining_market = book.submit(market_order_1).remaining_quantity;

    println!("Taker remaining: {}", remaining);
    println!("Market order remaining: {}", remaining_market);

    book.print_order_book();
}
