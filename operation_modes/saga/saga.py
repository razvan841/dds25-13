




class SagaOrchestrator:
    """
    Existing saga-based orchestration extracted so the app can swap strategies.
    """

    def __init__(
        self,
        db,
        logger,
        fetch_order_fn: Callable[[str], object],
        checkout_deadline_seconds: int,
    ):
        self.db = db
        self.logger = logger
        self.fetch_order = fetch_order_fn
        self.checkout_deadline_seconds = checkout_deadline_seconds

    # ---------- HTTP-layer operations ----------
    def checkout(self, order_id: str, order_entry, items_quantities: dict[str, int]):
        
        #Steps:
        #1. Verify if order is new (idempotency)
            # 1.1 log the order received

            # 1.2 Generate idempotency id for stock and payment to distinguish between checkouts
            # 1.2 log idempotency id 

            # 1.3 Create a saga and log it in db.

            # 1.4 Create prepare message for stock
            # 1.4 Log message for stock 
            # 1.4 Log compensation message for stock
            # 1.4 Send message to stock

            # 1.5 Create prepare message for payment
            # 1.5 Log message for payment 
            # 1.5 Log compensation message for stock
            # 1.5 Send message to payment            

        #2 else if order is already processed 
            # return
        
        #3 Maybe, maybe -> loop to check saga status untill saga is completed/failed
        
        
        
        
        
        correlation_id = str(uuid.uuid4())
        deadline_ts = (
            datetime.now(timezone.utc) + timedelta(seconds=self.checkout_deadline_seconds)
        ).timestamp()

        # Group items by their stock shard so each shard gets only its own items.
        items_by_shard: dict[int, list] = {}
        for item_id, qty in items_quantities.items():
            shard = compute_shard(item_id)
            items_by_shard.setdefault(shard, []).append((item_id, qty))


        create_saga(self.db, order_id, correlation_id, deadline_ts)
        set_stock_shards(self.db, order_id, list(items_by_shard.keys()))

        # Funds reserve command
        env_funds = make_envelope(
            "ReserveFundsCommand",
            transaction_id=order_id,
            payload=to_builtins(ReserveFundsCommand(user_id=order_entry.user_id, amount=order_entry.total_cost)),
            correlation_id=correlation_id,
        )
        append_outbox(self.db, order_id, PAYMENT_COMMANDS, env_funds)

        # One ReserveStockCommand per shard
        for shard, shard_items in items_by_shard.items():
            env_stock = make_envelope(
                "ReserveStockCommand",
                transaction_id=order_id,
                payload={"items": shard_items},
                correlation_id=correlation_id,
                reply_topic=STOCK_EVENTS,
            )
            append_outbox(self.db, order_id, f"stock.commands.{shard}", env_stock)

        # Block until the saga reaches a terminal state or the deadline is exceeded``.
        terminal = (STATUS_COMMITTED, STATUS_CANCELLED, STATUS_FAILED)
        deadline_at = time.monotonic() + self.checkout_deadline_seconds
        saga_status = STATUS_TRYING
        while time.monotonic() < deadline_at:
            saga = get_saga(self.db, order_id) or {}
            saga_status = saga.get("status", STATUS_TRYING)
            if saga_status in terminal:
                break
            time.sleep(0.05)

        if saga_status == STATUS_COMMITTED:
            return jsonify({"order_id": order_id, "status": saga_status}), 200
        if saga_status in (STATUS_CANCELLED, STATUS_FAILED):
            return jsonify({"order_id": order_id, "status": saga_status}), 400
        return jsonify({"order_id": order_id, "status": STATUS_TRYING}), 202
#=============================================================================



    def checkout_status(self, order_id: str):
        saga = get_saga(self.db, order_id)
        if saga is None:
            abort(404, f"Saga for order {order_id} not found")
        return jsonify({"order_id": order_id, "status": saga.get("status", STATUS_FAILED)})

    # ---------- Internal helpers ----------
    def _try_finalise_cancellation(self, order_id: str) -> None:
        """Set STATUS_CANCELLED only once payment AND all stock shards are fully resolved."""
        if not is_payment_resolved(self.db, order_id):
            return
        if not all_stock_shards_resolved(self.db, order_id):
            return
        set_status(self.db, order_id, STATUS_CANCELLED)
    
    # ---------- Kafka event handling ----------
    def handle_event(self, envelope):
        """
        Lightweight event handler updating saga state and issuing follow-up commands.
        """
        order_id = envelope.transaction_id
        msg_type = envelope.type

        if is_processed(self.db, order_id, envelope.message_id):
            return

        def publish_commit_if_ready():
            saga = get_saga(self.db, order_id) or {}
            pay_res = saga.get("payment_reservation_id", "")
            if not (pay_res and all_stock_reserved(self.db, order_id)):
                return
            set_status(self.db, order_id, STATUS_RESERVED)
            stock_reservations = get_stock_reservations(self.db, order_id)
            publish_envelope(
                PAYMENT_COMMANDS,
                key=order_id,
                envelope=make_envelope(
                    "CommitFundsCommand",
                    transaction_id=order_id,
                    payload=to_builtins(CommitFundsCommand(reservation_id=pay_res)),
                    correlation_id=envelope.correlation_id,
                    causation_id=envelope.message_id,
                ),
            )
            for shard, res_id in stock_reservations.items():
                publish_envelope(
                    f"stock.commands.{shard}",
                    key=order_id,
                    envelope=make_envelope(
                        "CommitStockCommand",
                        transaction_id=order_id,
                        payload=to_builtins(CommitStockCommand(reservation_id=res_id)),
                        correlation_id=envelope.correlation_id,
                        causation_id=envelope.message_id,
                        reply_topic=STOCK_EVENTS,
                    ),
                )

        match msg_type:
            case "OrderServicePing":
                self.logger.info("Received Kafka ping %s", envelope.message_id)
            case "FundsReservedEvent":
                payload = FundsReservedEvent(**envelope.payload)
                self.logger.warning(f"Received FundsReservedEvent")
                set_reservation_ids(self.db, order_id, payment_reservation_id=payload.reservation_id)
                saga = get_saga(self.db, order_id) or {}
                if saga.get("status") == STATUS_FAILED:
                    publish_envelope(
                        PAYMENT_COMMANDS,
                        key=order_id,
                        envelope=make_envelope(
                            "CancelFundsCommand",
                            transaction_id=order_id,
                            payload=to_builtins(CancelFundsCommand(reservation_id=payload.reservation_id)),
                            correlation_id=envelope.correlation_id,
                            causation_id=envelope.message_id,
                        ),
                    )
                else:
                    publish_commit_if_ready()
            case "StockReservedEvent":
                payload = StockReservedEvent(**envelope.payload)
                self.logger.warning(f"Received StockReservedEvent from shard {payload.shard_index}")
                add_stock_reservation(self.db, order_id, payload.shard_index, payload.reservation_id)
                if get_failing_flag(self.db, order_id):
                    # Failure already detected; cancel this stock reservation immediately
                    append_outbox(
                        self.db, order_id, f"stock.commands.{payload.shard_index}",
                        make_envelope(
                            "CancelStockCommand",
                            transaction_id=order_id,
                            payload=to_builtins(CancelStockCommand(reservation_id=payload.reservation_id)),
                            correlation_id=envelope.correlation_id,
                            causation_id=envelope.message_id,
                            reply_topic=STOCK_EVENTS,
                        ),
                    )
                else:
                    publish_commit_if_ready()
            case "FundsReserveFailedEvent":
                payload = FundsReserveFailedEvent(**envelope.payload)
                self.logger.warning("Funds reservation failed for %s: %s", order_id, payload.reason)
                set_failing_flag(self.db, order_id)
                mark_payment_resolved(self.db, order_id)
                # Cancel any stock shards that already responded
                stock_reservations = get_stock_reservations(self.db, order_id)
                for shard, res_id in stock_reservations.items():
                    append_outbox(
                        self.db, order_id, f"stock.commands.{shard}",
                        make_envelope(
                            "CancelStockCommand",
                            transaction_id=order_id,
                            payload=to_builtins(CancelStockCommand(reservation_id=res_id)),
                            correlation_id=envelope.correlation_id,
                            causation_id=envelope.message_id,
                            reply_topic=STOCK_EVENTS,
                        ),
                    )
                # Try to finalise — only sets CANCELLED when all stock shards
                # are also resolved (failed or cancel-confirmed)
                self._try_finalise_cancellation(order_id)
            case "StockReserveFailedEvent":
                payload = StockReserveFailedEvent(**envelope.payload)
                self.logger.warning("Stock reservation failed for %s: %s", order_id, payload.reason)
                set_failing_flag(self.db, order_id)
                # Mark this shard as resolved (it failed, no cancel needed for it)
                if payload.shard_index >= 0:
                    add_resolved_stock_shard(self.db, order_id, payload.shard_index)
                saga = get_saga(self.db, order_id) or {}
                pay_res = saga.get("payment_reservation_id", "")
                if pay_res and pay_res != "FAILED":
                    publish_envelope(
                        PAYMENT_COMMANDS,
                        key=order_id,
                        envelope=make_envelope(
                            "CancelFundsCommand",
                            transaction_id=order_id,
                            payload=to_builtins(CancelFundsCommand(reservation_id=pay_res)),
                            correlation_id=envelope.correlation_id,
                            causation_id=envelope.message_id,
                        ),
                    )
                # Cancel any other stock shards that already responded
                stock_reservations = get_stock_reservations(self.db, order_id)
                for shard, res_id in stock_reservations.items():
                    append_outbox(
                        self.db, order_id, f"stock.commands.{shard}",
                        make_envelope(
                            "CancelStockCommand",
                            transaction_id=order_id,
                            payload=to_builtins(CancelStockCommand(reservation_id=res_id)),
                            correlation_id=envelope.correlation_id,
                            causation_id=envelope.message_id,
                            reply_topic=STOCK_EVENTS,
                        ),
                    )
                # Try to finalise — only sets CANCELLED when payment and all
                # remaining stock shards are also resolved
                self._try_finalise_cancellation(order_id)
            case "FundsCommittedEvent":
                self.logger.warning(f"Received FundsCommittedEvent")
                set_committed_flags(self.db, order_id, funds_committed=True)
                funds_committed, _ = get_committed_flags(self.db, order_id)
                if funds_committed and all_stock_shards_committed(self.db, order_id):
                    set_status(self.db, order_id, STATUS_COMMITTED)
                    order_entry = self.fetch_order(order_id)
                    order_entry.paid = True
                    self.db.set(order_id, msgpack.encode(order_entry))
            case "StockCommittedEvent":
                self.logger.warning(f"Received StockCommittedEvent")
                payload = StockCommittedEvent(**envelope.payload)
                # Find which shard this reservation belongs to
                stock_reservations = get_stock_reservations(self.db, order_id)
                committed_shard = next(
                    (s for s, r in stock_reservations.items() if r == payload.reservation_id), None
                )
                if committed_shard is not None:
                    mark_stock_shard_committed(self.db, order_id, committed_shard)
                funds_committed, _ = get_committed_flags(self.db, order_id)
                if funds_committed and all_stock_shards_committed(self.db, order_id):
                    set_status(self.db, order_id, STATUS_COMMITTED)
                    order_entry = self.fetch_order(order_id)
                    order_entry.paid = True
                    self.db.set(order_id, msgpack.encode(order_entry))
            case "FundsCancelledEvent":
                self.logger.warning("Received FundsCancelledEvent")
                mark_payment_resolved(self.db, order_id)
                self._try_finalise_cancellation(order_id)
            case "StockCancelledEvent":
                self.logger.warning("Received StockCancelledEvent")
                payload = StockCancelledEvent(**envelope.payload)
                # Find which shard this cancellation belongs to and mark it resolved
                stock_reservations = get_stock_reservations(self.db, order_id)
                cancelled_shard = next(
                    (s for s, r in stock_reservations.items() if r == payload.reservation_id), None
                )
                if cancelled_shard is not None:
                    add_resolved_stock_shard(self.db, order_id, cancelled_shard)
                self._try_finalise_cancellation(order_id)
                
                
            case "PaymentServicePing":
                self.logger.info("Received payment ping %s", envelope.message_id)

            case _:
                self.logger.debug("Unhandled event type %s", msg_type)

        mark_processed(self.db, order_id, envelope.message_id)
