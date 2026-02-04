# import asyncio
# import logging
# import random
#
# wlogger = logging.getLogger("delta_data_layer.ingestor_manager.worker")
#
#
# class FileCopyWorker:
#     """Worker managing a queue of HadoopFileSystem tasks."""
#     def __init__(self, queue: asyncio.Queue, idle_timeout: int = 10, max_retries: int = 3):
#         self.queue = queue
#         self.idle_timeout = idle_timeout
#         self.max_retries = max_retries
#         self.fs = HadoopFileSystem()
#         self._running = True
#
#     async def _copy_with_retries(self, local_path: str, dest_path: str):
#         """Intenta copiar amb reintents exponencials."""
#         for attempt in range(1, self.max_retries + 1):
#             try:
#                 self.fs.copy_from_local(dest_path, local_path)
#                 wlogger.info(f"✅ Copiat correctament {local_path} → {dest_path}")
#                 return True
#             except Exception as e:
#                 wait = 2 ** attempt + random.uniform(0, 1)
#                 wlogger.warning(f"⚠️ Error copiant {local_path} (int {attempt}/{self.max_retries}): {e}. "
#                                f"Reintentant en {wait:.1f}s.")
#                 await asyncio.sleep(wait)
#         wlogger.error(f"❌ Error definitiu copiant {local_path} després de {self.max_retries} intents.")
#         return False
#
#     async def run(self):
#         """Executa el worker fins que la cua queda buida durant un temps."""
#         wlogger.info("Worker iniciat...")
#         while self._running:
#             try:
#                 item = await asyncio.wait_for(self.queue.get(), timeout=self.idle_timeout)
#             except asyncio.TimeoutError:
#                 wlogger.info("Cua buida. Worker aturat per inactivitat.")
#                 self._running = False
#                 break
#
#             local_path, dest_path = item
#             await self._copy_with_retries(local_path, dest_path)
#             self.queue.task_done()
#
#         wlogger.info("Worker finalitzat.")
