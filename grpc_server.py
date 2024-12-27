import sys
import os
import asyncio
import uuid
import execution
import time
from concurrent import futures
from collections import defaultdict
from app.user_manager import UserManager
from app.model_manager import ModelFileManager
import main_pb2_grpc

sys.path.append(
    os.path.join(os.path.dirname(__file__), "..", "..")
)  # Add the path to the root directory of the project curr directory is storage/ComfyUI
import grpc
import main_pb2


class GetImageManager:
    def __init__(self):
        self._callbacks = defaultdict(list)

    async def register_callback(self, prompt_id, callback):
        self._callbacks[prompt_id] = callback

    async def execute(self, prompt_id, outputs):
        if self._callbacks.get(prompt_id) is not None:
            await self._callbacks[prompt_id](outputs)
            del self._callbacks[prompt_id]


image_manager = GetImageManager()


class GetImageServicer(main_pb2_grpc.GetImageServiceServicer):
    def __init__(self):
        GetImageServicer.instance = self
        self.user_manager = UserManager()
        self.model_file_manager = ModelFileManager()
        self.prompt_queue = None
        self.number = 0
        self.cleanup_interval = 300  # 5분
        self.cleanup_task = None

    async def init_cleanup(self):
        self.cleanup_task = asyncio.create_task(self.periodic_cleanup())

    async def GetImage(self, request, context):
        await context.send_message(
            main_pb2.GetImageResponse(
                status=main_pb2.GetImageResponse.Status.PROCESSING
            )
        )
        json_string = request.prompt
        prompt = await json_string.json()
        valid = execution.validate_prompt(prompt)
        if valid[0]:
            prompt_id = str(uuid.uuid4())
            outputs_to_execute = valid[2]
            number = self.number
            self.number += 1
            extra_data = {"extra_pnginfo": prompt_id}
            self.prompt_queue.put(
                (number, prompt_id, prompt, extra_data, outputs_to_execute)
            )
        else:
            await context.send_message(
                main_pb2.GetImageResponse(
                    status=main_pb2.GetImageResponse.FAILED,
                    message=f"Invalid prompt: {valid[1]}",
                )
            )

        result_received = asyncio.Future()

        async def callback(outputs):
            await context.send_message(
                main_pb2.GetImageResponse(
                    status=main_pb2.GetImageResponse.Status.COMPLETED,
                    image_data=outputs,
                )
            )
            result_received.set_result(True)

        await image_manager.register_callback(prompt_id, callback)

        try:
            await result_received
        except Exception as e:
            await context.send_message(
                main_pb2.GetImageResponse(
                    status=main_pb2.GetImageResponse.FAILED,
                    message=str(e),
                )
            )

    async def periodic_cleanup(self):
        while True:
            await asyncio.sleep(self.cleanup_interval)
            # 오래된 콜백 정리
            current_time = time.time()
            for prompt_id in list(self._callbacks.keys()):
                if (
                    current_time - self._callbacks_time.get(prompt_id, 0)
                    > self.cleanup_interval
                ):
                    del self._callbacks[prompt_id]


async def image_generation_executor(prompt_id, image_data):
    await image_manager.execute(prompt_id, image_data)


async def serve():
    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))

    servicer = GetImageServicer.instance
    main_pb2_grpc.add_GetImageServiceServicer_to_server(servicer, server)

    await servicer.init_cleanup()

    listen_address = "[::]:50051"
    server.add_insecure_port(listen_address)

    await server.start()

    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        await server.stop(0)
    finally:
        await server.stop(0)
        for task in asyncio.all_tasks():
            task.cancel()
