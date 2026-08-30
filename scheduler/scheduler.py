from apscheduler.schedulers.background import BackgroundScheduler
import pytz
import logging
import os
from datetime import datetime
from rich.console import Console

logger = logging.getLogger(__name__)
console = Console()

class TradingScheduler:
    def __init__(self, system_controller):
        self.scheduler = BackgroundScheduler(timezone=pytz.timezone('Asia/Kolkata'))
        self.system_controller = system_controller

        # Hardcoded NSE holidays (example for 2024-2025)
        self.holidays = [
            "2024-01-26", "2024-03-08", "2024-03-25", "2024-03-29",
            "2024-04-11", "2024-04-17", "2024-05-01", "2024-06-17",
            "2024-07-17", "2024-08-15", "2024-10-02", "2024-11-01",
            "2024-11-15", "2024-12-25",
            # Add 2025 holidays
            "2025-01-26", "2025-02-26", "2025-03-14", "2025-03-31",
            "2025-04-10", "2025-04-14", "2025-04-18", "2025-05-01",
            "2025-08-15", "2025-08-27", "2025-10-02", "2025-10-21",
            "2025-10-22", "2025-11-05", "2025-12-25"
        ]

    def is_holiday(self):
        today_str = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d")
        return today_str in self.holidays

    def start_system_job(self):
        if self.is_holiday():
            logger.info("Today is a holiday. Skipping system start.")
            return
        logger.info("Starting trading system for the day...")
        self.system_controller.start()

    def stop_system_job(self):
        if self.is_holiday():
            return
        logger.info("Stopping trading system for the day...")
        self.system_controller.stop()

    def generate_report_job(self):
        if self.is_holiday():
            return
        logger.info("Generating daily PnL report...")
        self.system_controller.generate_report()

    def trigger_twelve_thirty_five_job(self):
        if self.is_holiday():
            return
        now = datetime.now(pytz.timezone('Asia/Kolkata'))
        if now.weekday() >= 5:
            return
        market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
        market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
        if not (market_open <= now <= market_close):
            return

        try:
            ok, message = self.system_controller.trigger_time_based_strategy("twelve_thirty_five")
            if ok:
                logger.info("12:35 trigger executed: %s", message)
            else:
                logger.info("12:35 trigger skipped: %s", message)
        except Exception as exc:
            logger.error("12:35 trigger job failed: %s", exc)

    def capture_oi_snapshot_job(self):
        if self.is_holiday():
            return
        now = datetime.now(pytz.timezone('Asia/Kolkata'))
        if now.weekday() >= 5:
            return
        market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
        market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
        if not (market_open <= now <= market_close):
            return

        try:
            from dashboard.dashboard import capture_oi_snapshot
            captured = capture_oi_snapshot()
            if captured:
                logger.info("Captured %s OI snapshot(s).", len(captured))
        except Exception as exc:
            logger.error("OI snapshot capture job failed: %s", exc)

    @staticmethod
    def _snapshot_interval_minutes():
        raw = str(os.getenv("OI_SNAPSHOT_INTERVAL_MINUTES", "5")).strip()
        try:
            value = int(raw)
        except Exception:
            value = 5
        return max(1, min(value, 30))

    def start(self):
        # 9:00 AM Mon-Fri
        self.scheduler.add_job(self.start_system_job, 'cron', day_of_week='mon-fri', hour=9, minute=0)

        # 3:35 PM Mon-Fri
        self.scheduler.add_job(self.stop_system_job, 'cron', day_of_week='mon-fri', hour=15, minute=35)

        strategies_enabled = bool(getattr(self.system_controller, "enable_strategies", True))
        if strategies_enabled:
            # 12:35 PM Mon-Fri
            self.scheduler.add_job(self.trigger_twelve_thirty_five_job, 'cron', day_of_week='mon-fri', hour=12, minute=35)

            # 3:25 PM Mon-Fri - close the same strategy even if the option legs stop ticking
            self.scheduler.add_job(self.trigger_twelve_thirty_five_job, 'cron', day_of_week='mon-fri', hour=15, minute=25)
        else:
            logger.info("Strategy scheduler jobs disabled (ENABLE_STRATEGIES=false).")

        # 4:00 PM Mon-Fri
        self.scheduler.add_job(self.generate_report_job, 'cron', day_of_week='mon-fri', hour=16, minute=0)

        # Every N minutes during market hours so OI and change-in-OI stay fresh.
        snapshot_interval = self._snapshot_interval_minutes()
        minute_expr = "*" if snapshot_interval == 1 else f"*/{snapshot_interval}"
        self.scheduler.add_job(
            self.capture_oi_snapshot_job,
            'cron',
            day_of_week='mon-fri',
            hour='9-15',
            minute=minute_expr,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=max(50, snapshot_interval * 60 - 10),
        )

        self.scheduler.start()
        logger.info("Scheduler started.")

        # Check if we are currently within market hours to start immediately
        now = datetime.now(pytz.timezone('Asia/Kolkata'))
        if now.weekday() < 5 and not self.is_holiday():
            market_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
            market_close = now.replace(hour=15, minute=35, second=0, microsecond=0)
            if market_open <= now < market_close:
                logger.info("Current time is within market hours. Starting system immediately.")
                self.start_system_job()
                self.capture_oi_snapshot_job()
                if strategies_enabled:
                    if now.time() >= now.replace(hour=12, minute=35, second=0, microsecond=0).time() and now.time() < now.replace(hour=12, minute=40, second=0, microsecond=0).time():
                        self.trigger_twelve_thirty_five_job()
                    if now.time() >= now.replace(hour=15, minute=25, second=0, microsecond=0).time() and now.time() < now.replace(hour=15, minute=30, second=0, microsecond=0).time():
                        self.trigger_twelve_thirty_five_job()
            else:
                force_after_hours = str(
                    os.getenv("FORCE_START_OUTSIDE_MARKET", "false")
                ).strip().lower() in {"1", "true", "yes", "y", "on"}
                if force_after_hours:
                    logger.info(
                        "Outside market hours. FORCE_START_OUTSIDE_MARKET is enabled, starting system for diagnostics."
                    )
                    self.start_system_job()
                else:
                    logger.info(
                        "Outside market hours (09:00-15:35 IST scheduler window). Live engine will stay idle until next session."
                    )

    def stop(self):
        self.scheduler.shutdown()
        logger.info("Scheduler stopped.")
