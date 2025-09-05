import ingestion.crawl_homeshop as cr_hs
import ingestion.crawl_kok as cr_k
import preprocessing.preprocessing_hs as pr_hs
import preprocessing.preprocessing_kok as pr_k
import embedding.embedding as emb
import utils.utils as utils
from concurrent.futures import ProcessPoolExecutor, wait, ALL_COMPLETED
import multiprocessing
import traceback
import os
import sys
from classifying import fct_to_cls, predict_main

