from ceci import PipelineStage
from scm_pipeline import ASDFFile, TextFile, Directory

class Destripe(PipelineStage):
    """
    This pipeline element is for removing noise stripes from mosaic images.
    """

    name = "Destripe"
    # Inputs to this stage. Format: List of (name, datatype) (see: scm_pipeline/data_types)
    inputs = [("images", Directory), ("manifest_file", TextFile)]
    # Outputs from this stage. Format: List of (name, datatype)
    outputs = [("destriped_images", Directory), ("manifest_file", TextFile)]  # KL Maybe we want to update the manifest file to exclude destriping anomaly images from coadds?
    # Config options -- these should match the config options for this stage in config.yaml. Format: {name: dtype}
    config_options = {"something": float}

    def run(self):
        # Retrieve configuration:
        my_config = self.config
        print("Here is my configuration :", my_config)

       
        path_to_images = self.get_input("images")
        manifest_file = self.get_input("manifest_file")
        print(f" Destripe Stage reading images from {path_to_images} according to manifest file {manifest_file}")
        blah = process(filename)

        path_to_images = self.get_output("destriped_images")
        manifest_file = self.get_output("manifest_file")
        print(f" Destripe Stage writing destriped images to to {path_to_images}")
        print(f"Manifest file {manifest_file} updated.")
        open(filename, "w").write(blah)

class PSFSplit(PipelineStage):
    """
    This pipeline element is for selecting stars for psf fitting
    """

    name = "SelectionStage"
    inputs = [("destriped_images", Directory)]
    outputs = [("star_catalog", ParquetFile)]
    config_options = {"magnitude_cut": float}

    def run(self):
        # Retrieve configuration:
        my_config = self.config
        print("Here is my configuration :", my_config)

       
        filename = self.get_input("object_catalog")
        print(f" SelectionStage reading from {filename}")
        blah = process(filename)

        filename = self.get_output("star_catalog")
        print(f" SelectionStage writing to {filename}")
        open(filename, "w").write(blah)




if __name__ == "__main__":
    cls = PipelineStage.main()
