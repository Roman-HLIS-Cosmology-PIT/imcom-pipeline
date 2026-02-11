from scm_pipeline import PipelineStage
from scm_pipeline.data_types import ASDFFile, TextFile, Directory, JSONFile, FitsFile # KL We need to add JSONFile
from .utils import make_imcom_config, import_dask 
import pyimcom
from roman_hlis_l2_driver.destripe_interface.destripe import destripe_all_layers
from roman_hlis_l2_driver.outliers.outlier_flagging import OutlierMap
import os
import numpy as np

class ConfigConversion(PipelineStage):
    """
    This pipeline element is for converting ceci-style config yaml files to pyimcom-style config files.
    """

    name = "ConfigConversion"
    inputs = []
    outputs = [("imcom_config", JSONFile)]
    config_options = {}

    def run(self):
        # Retrieve configuration:
        my_config = self.config
        print("Here is my configuration :", my_config)
        
        make_imcom_config(my_config['global'], "imcom_config.json")

        filename = self.get_output("imcom_config.json")
        print(f"ConfigConversion Stage wrote imcom Config to {filename}")
        

class Destripe(PipelineStage):
    """
    This pipeline element is for removing noise stripes from mosaic images.
    To do: Figure out if we need get_output ??
    """

    name = "Destripe"
    inputs = [("manifest_file", TextFile), ("imcom_config", JSONFile)]
    outputs = [("destriped_dir", Directory)]  # KL Maybe we want to update the manifest file to exclude destriping anomaly images from coadds?
    # Config options -- these should match the config options for this stage in config.yaml. Format: {"name": dtype}
    # actually maybe we should use the manifest file and output the destriped images into a mosaic directory?
    config_options = {}

    def run(self):
        # Retrieve Setupp:
        path_to_images = self.get_input("image_dir")
        manifest_file = self.get_input("manifest_file")
        imcom_config = self.get_input("imcom_config")
        print(f" Destripe Stage reading images from {path_to_images} according to manifest file {manifest_file}")
        print("IMCOM Config:", imcom_config)
        
        # Run destriping
        destripe_all_layers(imcom_config, verbose=True)
        OutlierMap(imcom_config, max_workers=27, run_and_save=False)

        output_dir = imcom_config["DSOUT"][0] 
        
        path_to_images = self.get_output("destriped_dir")
        print(f" Destripe Stage writing destriped images to to {path_to_images}")


class PSFSplit(PipelineStage):
    """
    This pipeline element is for performing PSF splitting on images. 
    To do: 
    - We might combine this with the BuildLayers stage...
    - Implement the actual PSF splitting functionality
    """

    name = "PSFSplit"
    inputs = [("destriped_dir", Directory), ("imcom_config", JSONFile), ("manifest_file", TextFile)]
    outputs = [("image_dir", Directory)]
    config_options = {}

    def run(self):
        # Retrieve configuration:
        imcom_config = self.get_input("imcom_config")
       
        destriped_dir = self.get_input("destriped_dir")
        print(f"PSFSplit Stage reading from {destriped_dir}")
        # Do the PSF Splitting

        filename = self.get_output("image_dir")
        print(f" PSFSplit Stage writing to {filename}")


class BuildLayers(PipelineStage):
    """
    Builds image layers for IMCOM processing.
    """

    name = "BuildLayers"
    inputs = [("image_dir",Directory), ("imcom_config", JSONFile)]
    outputs = []
    config_options = {} # MG Unsure

    def run(self):
        # Retrieve configuration:
        imcom_config = self.get_input("imcom_config")
        cfg = pyimcom.config.Config(cfg_file=imcom_config)
        image_dir = self.get_input("image_dir")
        print(f" BuildLayers Stage reading images from {image_dir}")

        # Actually draw the layers
        workers = os.cpu_count()
        pyimcom.layer.build_all_layers(cfg, image_dir, workers)

        print(f"BuildLayers Stage wrote images with all IMCOM layers to the InLayerCache")


class ImcomInitial(PipelineStage):
    """
    This stage performs the initial IMCOM processing on the prepared image layers.
    To do:
    - Implement the actual IMCOM processing functionality
    """
    name = "ImcomInitial"
    dask_parallel = True

    inputs = [("imcom_inputs_dir",Directory), ("imcom_config", JSONFile), ("psf_model", FitsFile)]
    # In pyimcom.coadd.Block the PSF model gets read in from the path given in the imcom_config. 
    # Some options here are:
        # 1) We just manually make sure the imcom_config has the right path to the PSF model
        # 2) We read in the PSF model here as an input and append it into the imcom_config
        # 3) In the ConfigConversion stage, we read in the PSF and any other inputs from other pipeline
        #    sections and append them into the imcom config
        
    outputs = [("imcom_outputs_dir",Directory)]
    config_options = {} 

    def coadd_range(self, cfg, brange, last=False):
        """
        Helper function to coadd a range of blocks.

        Parameters
        ----------
        cfg : pyimcom.config.Config
            The IMCOM configuration.
        brange : tuple
            The range of blocks to coadd (start, end).
        last : bool
            Whether this is the last range to be coadded.

        Returns
        -------
        None
        """
        start = brange[0]
        end = brange[1] if not last else brange[1] + 1

        for block in range(start, end):
            pyimcom.coadd.Block(cfg=cfg, this_sub=block)

        print(f"Completed coadding blocks {brange[0]} to {end}")

    def run(self):
        # Retrieve and setup configuration:
        imcom_config = self.get_input("imcom_config")
        cfg = pyimcom.config.Config(cfg_file=imcom_config)

        if not cfg.EVIL_IMCOM:
            dask, _ = import_dask.import_dask()

            block_dim = imcom_config["BLOCK"]
            n_block = block_dim ** 2
            # Block size (arcsec): output pixel scale X postage stamp pxl width  X N postage stamp width per block
            block_size = imcom_config["OUTSIZE"][2] * imcom_config["OUTSIZE"][1] * imcom_config["OUTSIZE"][0]

            mosaic_strips = np.ceil(636 / block_size) + 1
            print(f"Breaking up the mosaic into {mosaic_strips} strips of blocks to run in parallel.")

            block_ranges = [(i*block_size, (i+1)*block_size) for i in range(int(mosaic_strips))]
            if block_ranges[-1][1] > n_block:
                block_ranges[-1] = (block_ranges[-1][0], n_block)
            print(f"Block ranges for processing: {block_ranges}")

            # Run Imcom 1 in parallel over the block ranges using dask delayed
            delay_results = []
            for brange in block_ranges:
                delay_results.append(dask.delayed(self.coadd_range)(cfg, brange, last=(brange[1] == n_block)))
            
            results = dask.compute(*delay_results)
            print("Completed initial IMCOM processing for all blocks.")
            
        else:
            # Placeholder for EVIL IMCOM processing
            # We still need to add this flag to pyimcom config
            # And then implement the actual process here
            print("Running EVIL IMCOM - not yet implemented.")

        print(f"ImcomInitial Stage wrote IMCOM Iteration 1 images to the InLayerCache")


class ImSubtract(PipelineStage):
    """
    This stage runs imsubtract and updates the image cubes accordingly.
    """

    name = "imsubtract"
    inputs = [("imcom_config", JSONFile)]
    outputs = []
    config_options = {} 

    def run(self):
        # Retrieve configuration:
        imcom_config = self.get_input("imcom_config") 
        imcom_outputs_dir = self.get_input("imcom_outputs_dir")
        print(f" ImSubtract Stage reading images from {imcom_outputs_dir}")
       
        workers = os.cpu_count()

        pyimcom.splitpsf.imsubtract.run_imsubtract_all(imcom_config, workers)  # Temp files save to inlayercache
        pyimcom.splitpsf.update_cube.update(imcom_config)  # Update image cubes for next round of imcom

class ImcomFinal(PipelineStage):
    """
    This stage performs the second iteration of IMCOM processing.
    To do:
    - Implement the actual IMCOM processing functionality
    - Do the outputs need to be in a different location than the previous run of imcom?
    """

    name = "ImcomFinal"
    inputs = [("imcom_inputs_dir_2",Directory), ("imcom_config", JSONFile), ("manifest_file", TextFile), ("psf_model", FitsFile)]
    outputs = [("final_imcom_outputs_dir",Directory)]
    config_options = {} 

    def run(self):
        # Retrieve configuration:
        imcom_config = self.get_input("imcom_config")
        imcom_inputs_dir_2 = self.get_input("imcom_inputs_dir_2")
        manifest_file = self.get_input("manifest_file")
        print(f" ImcomFinal Stage reading images from {imcom_inputs_dir_2}")

        # Perform IMCOM processing 

        final_imcom_outputs_dir = self.get_output("final_imcom_outputs_dir")
        print(f"ImcomFinal Stage wrote final IMCOM processed images to {final_imcom_outputs_dir}")


class GenerateOutputs(PipelineStage):
    """
    This stage compresses the final images and writes out the PDF report.
    To do:
    - Implement the actual processes

    """

    name = "GenerateOutputs"
    inputs = [("final_imcom_outputs_dir",Directory), ("imcom_config", JSONFile), ("manifest_file", TextFile)]
    outputs = [("final_output_dir",Directory)]
    config_options = {} 

    def run(self):
        # Retrieve configuration:
        imcom_config = self.get_input("imcom_config")
        final_imcom_outputs_dir = self.get_input("final_imcom_outputs_dir")
        manifest_file = self.get_input("manifest_file")
        print(f" GenerateOutputs Stage reading images from {final_imcom_outputs_dir}")

        # Compress the images
        # Write the report

        final_output_dir = self.get_output("final_output_dir")
        print(f"GenerateOutputs Stage wrote final output products to {final_output_dir}")

if __name__ == "__main__":
    cls = PipelineStage.main()
