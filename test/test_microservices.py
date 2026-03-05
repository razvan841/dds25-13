import unittest

import utils as tu


class TestMicroservices(unittest.TestCase):

    def test_stock(self):
        # Test /stock/item/create/<price>
        item: dict = tu.create_item(5)
        self.assertIn('item_id', item)

        item_id: str = item['item_id']

        # Test /stock/find/<item_id>
        item: dict = tu.find_item(item_id)
        self.assertEqual(item['price'], 5)
        self.assertEqual(item['stock'], 0)

        # Test /stock/add/<item_id>/<number>
        add_stock_response = tu.add_stock(item_id, 50)
        self.assertTrue(200 <= int(add_stock_response) < 300)

        stock_after_add: int = tu.find_item(item_id)['stock']
        self.assertEqual(stock_after_add, 50)

        # Test /stock/subtract/<item_id>/<number>
        over_subtract_stock_response = tu.subtract_stock(item_id, 200)
        self.assertTrue(tu.status_code_is_failure(int(over_subtract_stock_response)))

        subtract_stock_response = tu.subtract_stock(item_id, 15)
        self.assertTrue(tu.status_code_is_success(int(subtract_stock_response)))

        stock_after_subtract: int = tu.find_item(item_id)['stock']
        self.assertEqual(stock_after_subtract, 35)

    def test_payment(self):
        # Test /payment/pay/<user_id>/<order_id>
        user: dict = tu.create_user()
        self.assertIn('user_id', user)

        user_id: str = user['user_id']

        # Test /users/credit/add/<user_id>/<amount>
        add_credit_response = tu.add_credit_to_user(user_id, 15)
        self.assertTrue(tu.status_code_is_success(add_credit_response))

        # add item to the stock service
        item: dict = tu.create_item(5)
        self.assertIn('item_id', item)

        item_id: str = item['item_id']

        add_stock_response = tu.add_stock(item_id, 50)
        self.assertTrue(tu.status_code_is_success(add_stock_response))

        # create order in the order service and add item to the order
        order: dict = tu.create_order(user_id)
        self.assertIn('order_id', order)

        order_id: str = order['order_id']

        add_item_response = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))

        add_item_response = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))
        add_item_response = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))

        payment_response = tu.payment_pay(user_id, 10)
        self.assertTrue(tu.status_code_is_success(payment_response))

        credit_after_payment: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit_after_payment, 5)

    def test_order(self):
        # Test /payment/pay/<user_id>/<order_id>
        user: dict = tu.create_user()
        self.assertIn('user_id', user)

        user_id: str = user['user_id']

        # create order in the order service and add item to the order
        order: dict = tu.create_order(user_id)
        self.assertIn('order_id', order)

        order_id: str = order['order_id']

        # add item to the stock service
        item1: dict = tu.create_item(5)
        self.assertIn('item_id', item1)
        item_id1: str = item1['item_id']
        add_stock_response = tu.add_stock(item_id1, 15)
        self.assertTrue(tu.status_code_is_success(add_stock_response))

        # add item to the stock service
        item2: dict = tu.create_item(5)
        self.assertIn('item_id', item2)
        item_id2: str = item2['item_id']
        add_stock_response = tu.add_stock(item_id2, 1)
        self.assertTrue(tu.status_code_is_success(add_stock_response))

        add_item_response = tu.add_item_to_order(order_id, item_id1, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))
        add_item_response = tu.add_item_to_order(order_id, item_id2, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))
        subtract_stock_response = tu.subtract_stock(item_id2, 1)
        self.assertTrue(tu.status_code_is_success(subtract_stock_response))

        checkout_response = tu.checkout_order(order_id).status_code
        self.assertTrue(tu.status_code_is_failure(checkout_response))

        stock_after_subtract: int = tu.find_item(item_id1)['stock']
        self.assertEqual(stock_after_subtract, 15)

        add_stock_response = tu.add_stock(item_id2, 15)
        self.assertTrue(tu.status_code_is_success(int(add_stock_response)))

        credit_after_payment: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit_after_payment, 0)

        checkout_response = tu.checkout_order(order_id).status_code
        self.assertTrue(tu.status_code_is_failure(checkout_response))

        add_credit_response = tu.add_credit_to_user(user_id, 15)
        self.assertTrue(tu.status_code_is_success(int(add_credit_response)))

        credit: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit, 15)

        stock: int = tu.find_item(item_id1)['stock']
        self.assertEqual(stock, 15)

        checkout_response = tu.checkout_order(order_id)
        print(checkout_response.text)
        self.assertTrue(tu.status_code_is_success(checkout_response.status_code))

        stock_after_subtract: int = tu.find_item(item_id1)['stock']
        self.assertEqual(stock_after_subtract, 14)

        credit: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit, 5)

    def test_saga_checkout(self):
        """
        End-to-end saga checkout test.

        Verifies that a full checkout via Kafka saga:
          1. Returns 202 (TRYING) immediately.
          2. Eventually reaches COMMITTED.
          3. Deducts credit from the user.
          4. Deducts stock from each item.
        """
        # --- Setup: user with enough credit ---------------------------------
        user = tu.create_user()
        user_id = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        # --- Setup: two items with sufficient stock -------------------------
        item1 = tu.create_item(10)   # price 10
        item2 = tu.create_item(20)   # price 20
        item_id1 = item1['item_id']
        item_id2 = item2['item_id']
        tu.add_stock(item_id1, 5)
        tu.add_stock(item_id2, 5)

        # --- Setup: order with both items -----------------------------------
        order = tu.create_order(user_id)
        order_id = order['order_id']
        self.assertTrue(tu.status_code_is_success(tu.add_item_to_order(order_id, item_id1, 2)))  # qty 2 → cost 20
        self.assertTrue(tu.status_code_is_success(tu.add_item_to_order(order_id, item_id2, 1)))  # qty 1 → cost 20

        # --- Checkout: saga starts, returns 202 -----------------------------
        resp = tu.checkout_order(order_id)
        self.assertIn(resp.status_code, (200, 202), msg=f"Checkout returned {resp.status_code}: {resp.text}")
        data = resp.json()
        self.assertIn(data.get('status'), ('TRYING', 'COMMITTED'), msg=f"Unexpected initial status: {data}")

        # --- Poll until saga completes --------------------------------------
        final_status = tu.poll_checkout_status(order_id, timeout=15.0)
        self.assertEqual(final_status, 'COMMITTED',
                         msg=f"Saga did not commit; final status was {final_status!r}")

        # --- Verify side effects --------------------------------------------
        credit_after = tu.find_user(user_id)['credit']
        self.assertEqual(credit_after, 60,
                         msg=f"Expected 60 credit after spending 40, got {credit_after}")

        stock1_after = tu.find_item(item_id1)['stock']
        self.assertEqual(stock1_after, 3,
                         msg=f"Expected 3 units of item1 remaining, got {stock1_after}")

        stock2_after = tu.find_item(item_id2)['stock']
        self.assertEqual(stock2_after, 4,
                         msg=f"Expected 4 units of item2 remaining, got {stock2_after}")

    def test_saga_checkout_insufficient_stock(self):
        """
        Compensation path: checkout should fail (FAILED/CANCELLED) and
        credit should be returned when stock is unavailable.
        """
        # --- Setup ----------------------------------------------------------
        user = tu.create_user()
        user_id = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        # Item with only 1 unit in stock
        item = tu.create_item(10)
        item_id = item['item_id']
        tu.add_stock(item_id, 1)

        order = tu.create_order(user_id)
        order_id = order['order_id']
        # Try to order 2 units (more than available)
        tu.add_item_to_order(order_id, item_id, 2)

        # --- Checkout should start then compensate --------------------------
        resp = tu.checkout_order(order_id)
        self.assertIn(resp.status_code, (200, 202), msg=f"Checkout returned {resp.status_code}")

        final_status = tu.poll_checkout_status(order_id, timeout=15.0)
        self.assertIn(final_status, ('FAILED', 'CANCELLED'),
                      msg=f"Expected saga to fail; got {final_status!r}")

        # Credit must be restored
        credit_after = tu.find_user(user_id)['credit']
        self.assertEqual(credit_after, 100,
                         msg=f"Credit should be fully restored; got {credit_after}")

        # Stock must be unchanged
        stock_after = tu.find_item(item_id)['stock']
        self.assertEqual(stock_after, 1,
                         msg=f"Stock should be unchanged; got {stock_after}")


if __name__ == '__main__':
    unittest.main()
