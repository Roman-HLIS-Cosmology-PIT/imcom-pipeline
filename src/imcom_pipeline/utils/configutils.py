from pyimcom import config
import json

def make_imcom_config(config_yaml, outname):
    """
    This function takes the ceci-style config yaml file from this pipeline repo and generates a 
    pyimcom-style config file for imcom.
    
    :param config_yaml: The ceci-style config yaml file
    :param output: The output pyimcom-style config file
    """

    config_dict = {
        # Section I : Input Files
        "OBSFILE": config_yaml["obs_file"],
        "INDATA": config_yaml["indata"],
        "FILTER": config_yaml["filter"],
        "INPSF":config_yaml["in_psf"],
        "PSFSPLIT": config_yaml["psf_split"],

        # Section II : Masks and Layers
        # "PMASK": config_yaml["p_mask"],
        # "CMASK": config_yaml["c_mask"],
        "EXTRAINPUT": config_yaml["extra_input"],
        # "LABNOISETHRESHOLD": config_yaml["lab_noise_threshold"],

        # Section III: Area to Coadd
        "CTR": config_yaml["ctr"],
        "LONPOLE": config_yaml["lonpole"],
        "BLOCK": config_yaml["block"],
        "OUTSIZE": config_yaml["outsize"],

        # Section IV: Postage Stamp Parameters
        "FADE": config_yaml["fade"],
        "PAD": config_yaml["pad"],
        "PADSIDES": config_yaml["padsides"],
        "STOP": config_yaml["stop"],

        # Section V: Output Settings
        "OUTMAPS": config_yaml["outmaps"],
        "OUT": config_yaml["out"],
        "TEMPFILE": config_yaml["tempfile"],
        "INLAYERCACHE": config_yaml["inlayer_cache"],

        # Section VI: Target Output PSF
        "NOUT": config_yaml["n_out"],
        "OUTPSF": config_yaml["out_psf"],
        "EXTRASMOOTH": config_yaml["extra_smooth"],

        # Section VII: Building Linear Systems
        "NPIXPSF": config_yaml["n_pix_psf"],
        "PSFCIRC": config_yaml["psf_circ"],
        "PSFNORM": config_yaml["psf_norm"],
        "AMPPENALTY": config_yaml["amp_penalty"],
        "FLATPEN": config_yaml["flat_penalty"],
        "PSFINTERP": config_yaml["psf_interp"],
        "INPAD": config_yaml["inpad"],

        # Section VIII : Solving Linear Systems
        "LAKERNEL": config_yaml["lakernel"],

        # Section IX: Destriping Params
        "DSMODEL": config_yaml["ds_model"],
        "DSOUT": config_yaml["ds_outputs"],
        "CGMODEL": config_yaml["cg_model"],
        "DSCOST": config_yaml["ds_cost"],
        "DSOBSFILE": config_yaml["ds_obsfile"],
        "DSRESTART": config_yaml["ds_restart"],
        "GAINDIR": config_yaml["gain_dir"],

        "KAPPAC": config_yaml["kappac"],
        "UCMIN": config_yaml["ucmin"],
        "SMAX": config_yaml["smax"],

        "TILESCHM": config_yaml["tileschm"],
        "RERUN": config_yaml["rerun"],
        "MOSAICID": config_yaml["mosaic"]

        }
    

    if config_yaml["n_out"] > 1:
        for i in range(config_yaml["n_out"]+1):
            if i==0: continue
            config_dict[f"OUTPSF{i+1}"] = config_yaml["outpsf_extra"][i]
            config_dict[f"EXTRASMOOTH{i+1}"] = config_yaml["sigmatarget_extra"][i]
        
    if config_yaml["linear_algebra"] == "Iterative":
        config_dict["ITERRTOL"] = config_yaml["iter_rtol"]
        config_dict["ITERMAX"] = config_yaml["iter_max"]
    elif config_yaml["linear_algebra"] == "Empirical":
        config_dict["EMPIRNQC"] = config_yaml["no_qlt_ctrl"]

    cfg = config.Config(cfg_file=json.dumps(config_dict))
    cfg.to_file(fname=outname)

    return cfg


def modify_config(cfg, options=None, fname=None):
    """
    This function takes a pyimcom config object and adds options to it from the other pipeline sections.

    Parameters
    ----------
    cfg: pyimcom.config.Config
        current config, to which we will add
    options: dict
        options to add to the config.
        If None, we will use the default options from the config yaml file.
    """

    if options is not None:
        for key, value in options.items():
            setattr(cfg, key, value)
    
    if fname is not None:
        cfg.to_file(fname=fname)

    return cfg