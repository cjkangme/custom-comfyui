from PIL import Image
from grpc_server import image_generation_executor
import asyncio

import numpy as np


class SendImage:
    @classmethod
    def INPUT_TYPES(s):
        return {
            "required": {"image": ("IMAGE",)},
            "hidden": {"extra_pnginfo": "EXTRA_PNGINFO"},
        }

    RETURN_TYPES = ()
    FUNCTION = "send_images"

    OUTPUT_NODE = True

    CATEGORY = "image"
    DESCRIPTION = "Send image to grpc server"

    def send_images(self, images, extra_pnginfo=None):
        results = []
        for tensor in images:
            array = 255.0 * tensor.cpu().numpy()
            image = Image.fromarray(np.clip(array, 0, 255).astype(np.uint8))

            asyncio.create_task(image_generation_executor(extra_pnginfo, image))
            results.append({"image": image})

        return {"ui": {"images": results}}


NODE_CLASS_MAPPINGS = {
    "SendImage": SendImage,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "SendImage": "(grpc) Send Image",
}
