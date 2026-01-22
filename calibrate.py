"""
This module provides a class `MaximaCalibrator` for calibrating 2D X-ray diffraction patterns. 
It utilizes a hierarchical Swin Transformer (`MaximaSwin`) to regress an initial geometric guess 
(distance, center, rotation) and subsequently refines this guess using `pyFAI` and a seeded 
geometric optimization strategy (`PeakOptimizer`).

Classes:
    MaximaSwin: The neural network architecture.
    PeakOptimizer: The refinement engine using peak detection.
    MaximaCalibrator: The main interface for loading models and running calibration.
"""

import argparse
import numpy as np
import torch
import torch.nn as nn
from torchvision import transforms
import fabio
from pyFAI.geometry import Geometry
from pyFAI.calibrant import CALIBRANT_FACTORY
from pyFAI.detectors import detector_factory
from pyFAI.geometryRefinement import GeometryRefinement
from skimage.feature import peak_local_max
from scipy.optimize import minimize
from transformers import SwinModel, SwinConfig

class MaximaSwin(nn.Module):
    """
    Main architecture object combining a Swin Transformer backbone with a regression head.

    Args:
        backbone (SwinModel): Pretrained Swin Transformer backbone.
        head (nn.Module): Fully connected head for geometry regression.
    """
    def __init__(self, backbone, head):
        super().__init__()
        self.swin = backbone
        self.head = head
        self.pooler = nn.AdaptiveAvgPool1d(1)

    def forward(self, pixel_values):
        """
        Forward pass to predict geometry parameters.

        Args:
            pixel_values (torch.Tensor): Input image tensor of shape (B, C, H, W).

        Returns:
            torch.Tensor: Predicted geometry parameters of shape (B, num_outputs).
        """

        outputs = self.swin(pixel_values=pixel_values)
        last_hidden_state = outputs.last_hidden_state             # shape: (B, N, C)
        feature_transpose = last_hidden_state.transpose(1, 2)     # shape: (B, C, N)
        feature_pool = self.pooler(feature_transpose).flatten(1)  # shape: (B, C)
        geometry_params = self.head(feature_pool)                 # shape: (B, num_outputs)

        return geometry_params

class PeakOptimizer:
    """
        Optimizes peak detection parameters and refines geometry using the Nelder-Mead method.

        Args:
            image (np.ndarray): 2D diffraction image for peak detection.
            initial_geometry (Geometry): Initial geometry estimate from the model.
            calibrant (Calibrant): pyFAI Calibrant object.
            exclude_border (int, optional): Border width to exclude from peak detection. Defaults to 300.
    """
    def __init__(self, image, initial_geometry, calibrant, exclude_border=300):
        self.image = image
        self.geo = initial_geometry
        self.calibrant = calibrant
        self.exclude_border = exclude_border
        
        self.best_error = float('inf')
        self.best_geometry = None

    def _objective(self, params):    
        min_dist = int(max(1, round(params[0])))
        thresh = np.clip(params[1], 0.001, 1.0)
        tol_deg = max(0.1, params[2])

        try:
            peaks = peak_local_max(
                self.image, 
                min_distance=min_dist, 
                threshold_rel=thresh,
                exclude_border=self.exclude_border
            )
        except Exception:
            return 1e6 

        if len(peaks) < 5: return 1e5

        tth_measured = self.geo.tth(peaks[:, 0], peaks[:, 1])
        tth_expected = np.array(self.calibrant.get_2th())
        diff_matrix = np.abs(tth_measured[:, None] - tth_expected[None, :])
        
        mask = diff_matrix.min(axis=1) < np.deg2rad(tol_deg)
        if np.sum(mask) < 6: return 1e4

        data = np.column_stack((peaks[mask, 0], peaks[mask, 1], diff_matrix.argmin(axis=1)[mask]))

        try:
            refiner = GeometryRefinement(
                data=data,  
                dist=self.geo.dist, poni1=self.geo.poni1, poni2=self.geo.poni2,
                rot1=self.geo.rot1, rot2=self.geo.rot2, rot3=self.geo.rot3,
                pixel1=self.geo.detector.pixel1, pixel2=self.geo.detector.pixel2,
                detector=self.geo.detector, wavelength=self.calibrant.wavelength,
                calibrant=self.calibrant
            )
            error = refiner.refine2()
            
            if error < self.best_error:
                self.best_error = error
                self.best_geometry = refiner.get_geometry()
            return error
        except Exception:
            return 1e6

    def optimize(self, initial_guess=[5, 0.1, 1.0]):
        """
        Runs the Nelder-Mead optimization to find the best refinement parameters.

        Args:
            initial_guess (list): [min_distance (px), threshold_rel, tolerance (deg)].

        Returns:
            Geometry: The best refined geometry object found.
        """
        _ = minimize(self._objective, x0=initial_guess, method='Nelder-Mead', tol=1e-4, options={'maxiter': 50})
        return self.best_geometry

class MaximaCalibrator:
    """
    Calibrator class using MaxSWIN model and pyFAI for diffraction geometry calibration.

    Args:
        model_path (str): Path to the trained model checkpoint (.pth).
        calibrant (str, optional): Alias of the calibrant. Defaults to 'alpha_Al2O3'.
        detector (str, optional): Alias of the detector. Defaults to 'Eiger2Cdte_1M'.
        wavelength (float, optional): X-ray wavelength in meters. Defaults to 0.512A.
        image_size (int, optional): Input image size for the model. Defaults to 1056.
        backbone (str, optional): Pretrained Swin model name. Defaults to Swin Base Patch 4.
        hidden_dim (int, optional): Hidden dimension size for the regression head. Defaults to 1024.
        device (str, optional): Computation device ('cpu' or 'cuda'). Defaults to auto-detect.
    """
    def __init__(self, 
                 model_path: str, 
                 calibrant: str = 'alpha_Al2O3', 
                 detector: str = 'Eiger2Cdte_1M', 
                 wavelength: float = 0.5121261413149675e-10,
                 image_size: int = 1056,
                 backbone: str = 'microsoft/swin-base-patch4-window12-384-in22k',
                 hidden_dim: int = 1024,
                 device: str = None):
        
        self.model_path = model_path
        self.calibrant_alias = calibrant
        self.detector_alias = detector
        self.wavelength = wavelength
        self.image_size = image_size
        self.backbone = backbone
        self.hidden_dim = hidden_dim
        
        self.device = torch.device(device) if device else torch.device("cuda" if torch.cuda.is_available() else "cpu")
        print(f"Initializing MaximaCalibrator on {self.device}...")
        self._load_resources()

    def _load_resources(self):
        """Loads and configures the pyFAI physics objects and the PyTorch model."""
        try:
            self.calibrant = CALIBRANT_FACTORY(self.calibrant_alias)
            self.calibrant.wavelength = self.wavelength
            self.detector = detector_factory(self.detector_alias)
        except KeyError as e:
            raise ValueError(f"Unknown detector or calibrant: {e}")

        self.model = self._build_model()
        self._load_weights()
        self.model.to(self.device)
        self.model.eval()

    def _build_model(self) -> nn.Module:
        """Instantiates the Swin Transformer architecture."""
        print(f"Building architecture: {self.backbone}")
        
        head = nn.Sequential(
            nn.Linear(self.hidden_dim, 512),
            nn.GELU(),
            nn.Dropout(0.1),
            nn.Linear(512, 6)
        )

        config = SwinConfig.from_pretrained(self.backbone)
        config.image_size = self.image_size
        backbone = SwinModel.from_pretrained(self.backbone, config=config, ignore_mismatched_sizes=True)
        
        return MaximaSwin(backbone, head)

    def _load_weights(self):
        """Loads state dict from the checkpoint file."""
        print(f"Loading weights from: {self.model_path}")
        state_dict = torch.load(self.model_path, map_location='cpu')
        try:
            self.model.load_state_dict(state_dict, strict=True)
        except RuntimeError as e:
            print(f"Strict load failed ({e}). Check checkpoint format.")

    def _image_to_tensor(self, image: np.ndarray) -> torch.Tensor:
        """Preprocesses numpy image into a normalized tensor."""
        image = np.log1p(image)
        img_min, img_max = image.min(), image.max() 
        image = (image - img_min) / (img_max - img_min) if img_max > img_min else np.zeros_like(image) 
        
        tensor = torch.from_numpy(image).unsqueeze(0).repeat(3, 1, 1).float() # make it 3-channel

        _, h, w = tensor.shape
        pad_h, pad_w = (max(h, w) - h) // 2, (max(h, w) - w) // 2
        
        transform = transforms.Compose([
            transforms.Pad((pad_w, pad_h)), 
            transforms.Resize((self.image_size, self.image_size), antialias=True), 
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]) # normalize to ImageNet stats
        ])
        return transform(tensor).unsqueeze(0)

    def calibrate(self, image_path: str, output_path: str = None) -> Geometry:
        """
        Runs the full calibration pipeline.

        Args:
            image_path (str): Path to the input diffraction image (e.g., .tif, .cbf).
            output_path (str, optional): Path to save the final geometry as a .poni file.

        Returns:
            Geometry: The final refined pyFAI Geometry object.
        """
        # run inference
        image = fabio.open(image_path).data.astype(np.float32)
        tensor = self._image_to_tensor(image).to(self.device)
        with torch.no_grad():
            pred = self.model(tensor).cpu().numpy().flatten()
        
        initial_geometry = Geometry(
            dist=pred[0], poni1=pred[1], poni2=pred[2],
            rot1=pred[3], rot2=pred[4], rot3=pred[5],
            wavelength=self.wavelength, detector=self.detector
        )

        # geometric refinement
        clipped_image = np.clip(image, 30.0, 300.0) # clip bg noise and zingers
        optimizer = PeakOptimizer(clipped_image, initial_geometry, self.calibrant)
        final_geometry = optimizer.optimize() or initial_geometry # returns initial geometry if optimization fails
        
        if output_path:
            final_geometry.save(output_path)
            
        return final_geometry

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MaximaCalibrator: MaxSWIN + pyFAI")
    parser.add_argument("--image", required=True, help="Input image file")
    parser.add_argument("--model", required=True, help="Model checkpoint (.pth)")
    parser.add_argument("--calibrant", default="alpha_Al2O3")
    parser.add_argument("--detector", default="Eiger2Cdte_1M")
    parser.add_argument("--wavelength", type=float, default=0.5121261413149675e-10)
    parser.add_argument("--output", default=None)
    
    args = parser.parse_args()

    calibrator = MaximaCalibrator(
        model_path=args.model,
        calibrant=args.calibrant,
        detector=args.detector,
        wavelength=args.wavelength
    )

    result = calibrator.calibrate(args.image, args.output)