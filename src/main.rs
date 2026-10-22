use orderbook_rs::orderbook::{Order, OrderBook, OrderType, Side};

fn main() {
    let mut book = OrderBook::new();

    let order1 = Order {
        id: 1,
        side: Side::Ask,
        price: 100,
        quantity: 5,
        timestamp: 1,
        order_type: OrderType::Limit,
    };
    book.submit_limit_order(order1);

    let taker = Order {
        id: 6,
        side: Side::Bid,
        price: 105,
        quantity: 10,
        timestamp: 2,
        order_type: OrderType::Limit,
    };

    let market_order_1 = Order {
        id: 7,
        side: Side::Ask,
        price: 0, // price is ignored for market orders
        quantity: 2,
        timestamp: 3,
        order_type: OrderType::Market,
    };

    let remaining = book.submit_limit_order(taker);

    let remaining_market = book.submit_market_order(market_order_1);

    println!("Taker remaining: {}", remaining);
    println!("Market order remaining: {}", remaining_market);

    book.print_order_book();
}
