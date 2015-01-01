import os
import h5py


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
        base_fname = ''.join(tail.split('.')[0:-2])

        # create list of files
        self.file_names = []
        for i in range(self.n_files):
            this_fname = '.'.join([base_fname, str(i), 'hdf5']) 
            full_fname = os.path.join(head, this_fname)
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
        if 'PartType' not in name and 'Header' not in name:
            # add appropriate keys and/or dicts to meta
            # this logic is for the nested 'Parameters' group
            if '/' in name:
                names = name.split('/') 
                if names[0] not in meta:
                    self.meta[names[0]] = {}
                self.meta[names[0]][names[1]] = dict(obj.attrs.items())
            # this logic handles 'Constants' and 'Units'
            else:
                self.meta[name] = dict(obj.attrs.items())




