import os
import h5py
import warnings
import numpy as np

class OwlsSnapshot(object):

    def __init__(self, fname):

        # check fname exists
        if not os.path.isfile(fname):
            raise IOError('fname={}, file not found'.format(fname))

        # read number of files in snapshot from fname
        with h5py.File(fname, 'r') as h5f:
            self.n_files = h5f['Header'].attrs['NumFilesPerSnapshot']

        # store head and tail of file path
        # tail will be of the form <string>.<file_num>.hdf5
        self._fname_head, self._fname_tail = os.path.split(fname)
        
        # extract string from tail
        base_fname = ''.join(self._fname_tail.split('.')[0:-2])

        # create list of files
        self.file_names = []
        for i in range(self.n_files):
            this_fname = '.'.join([base_fname, str(i), 'hdf5']) 
            full_fname = os.path.join(self._fname_head, this_fname)
            self.file_names.append(full_fname)
            if not os.path.isfile(full_fname):
                raise IOError('fname={}, file not found'.format(full_fname))

        # read non header meta data
        self.meta = {}
        with h5py.File(fname, 'r') as h5f:
            h5f.visititems(self._get_meta)

        # read headers
        self.headers = []
        for tmp_fname in self.file_names:
            with h5py.File(tmp_fname, 'r') as h5f:
                self.headers.append(dict(h5f['Header'].attrs.items()))


    def _get_meta(self, name, obj):
        """ Helper function for reading meta data.  Should not be called
        directly. """ 

        if 'PartType' not in name and 'Header' not in name:
            # add appropriate keys and/or dicts to meta
            # this logic is for the nested 'Parameters' group
            if '/' in name:
                names = name.split('/') 
                if names[0] not in self.meta:
                    self.meta[names[0]] = {}
                self.meta[names[0]][names[1]] = dict(obj.attrs.items())
            # this logic handles 'Constants' and 'Units'
            else:
                self.meta[name] = dict(obj.attrs.items())

                
    def _validate_dataset_name(self, dataset_name):
        """ Ensure dataset name is properly formatted. """ 

        if not dataset_name.startswith('PartType'):
            raise ValueError('dataset_name must begin with "PartType", '
                             'dataset_name={}'.format(dataset_name))

        ptype = int(dataset_name[len('PartType')])
        if ptype not in [0,1,4]:
            raise ValueError('particle type must be 0, 1, or 4, '
                             'dataset_name='+dataset_name+', '+
                             'ptype={}'.format(ptype))
            
        return ptype


    def read_dataset(self, dataset_name):
        """ Read a dataset from all files in the snapshot and return
        a numpy array. """ 

        ptype = self._validate_dataset_name(dataset_name)

        # create array of particle numbers in each file
        # (sometimes star particles will be in some files 
        # of a snapshot but not others)
        pcount = np.array([h['NumPart_ThisFile'][ptype] for h in self.headers])

        # dummy check
        npar = self.headers[0]['NumPart_Total'][ptype]
        assert(npar == np.sum(pcount))

        # raise exception if missing everywhere
        if np.sum(pcount) == 0: 
            raise ValueError('dataset does not exists in any files, '
                             'dataset_name='+dataset_name)

        # warn if missing from some files. 
        if np.any(pcount==0):
            warnings.warn('dataset missing from some files, '
                          'dataset_name='+dataset_name+', '+
                          'pcount={}'.format(pcount), UserWarning)

        # get dtype and shape from first file which contains
        # the dataset and initialize large numpy array
        ifinite, = np.where(pcount!=0)
        ifile = ifinite[0]
        with h5py.File(self.file_names[ifile], 'r') as h5f:
            ds = h5f[dataset_name]
            shape = list(ds.shape) # make it mutable
            shape[0] = npar
            dat = np.zeros(shape=shape, dtype=ds.dtype)
        assert(len(shape)==1 or len(shape)==2)

        # read and return
        ired = 0
        for ifile,fname in enumerate(self.file_names):
            if pcount[ifile] > 0:
                ii = ired
                ff = ired + pcount[ifile]
                with h5py.File(fname, 'r') as h5f:
                    dat[ii:ff,...] = h5f[dataset_name].value
                ired += pcount[ifile]
        return dat

    def read_dataset_1(self, ifile, dataset_name):
        """ Read a dataset from a single file in the snapshot and return
        a numpy array. """ 
        ptype = self._validate_dataset_name(dataset_name)
        with h5py.File(self.file_names[ifile], 'r') as h5f:
            dat = h5f[dataset_name].value
        return dat


