import glob
import numpy  as np
import tables as tb

## PARAMS
run = 7435
wf_dir     = f"/analysis/{run}/hdf5/data/"
event_data = f"/home/shifter/gdiaz/WF_selector/DATA/de_events/DE_events_{run}.npy"
out_dir    = f"/home/shifter/gdiaz/WF_selector/DATA/selected_rwfs/" 


# WF FILES AND EVENTS
wf_files   = glob.glob(wf_dir  + "/*trigger2*")
wf_files.sort()


## LOAD EVENTS
events = np.load( event_data )


## CREATE FILE TO SAVE RWFS AND EVENTS
h5file = tb.open_file(out_dir + "/" + f"ds_rwfselection_{run}.h5", mode="w", title="DS selection")
selrwfs_group    = h5file.create_group("/", "DS_RWFS")
event_info_group = h5file.create_group("/",  "Events")

PMTRWFs_Array  = h5file.create_earray(selrwfs_group, "pmtrwfs", 
                                     tb.Int16Atom(), shape=(0, 12, 64000))
SIPMRWFs_Array = h5file.create_earray(selrwfs_group, "sipmrwfs", 
                                      tb.Int16Atom(), shape=(0, 1792, 1600)) 

class Event_Info(tb.IsDescription):
    event = tb.Int32Col()
    time  = tb.UInt64Col()

Event_Info_table = h5file.create_table(event_info_group, "Event_Time", Event_Info, "Event_Time")
EI = Event_Info_table.row


## SELECT AND WRITE
for file in wf_files:
    WF_file = tb.open_file( file )

    pmtrwfs = WF_file.root.RD.pmtrwf 
    sipmwfs = WF_file.root.RD.sipmrwf

    events_time = WF_file.root.Run.events.read()
    events_in_file = events_time["evt_number"]
    sel = np.in1d(events_in_file, events)

    if sel.any():
        idxs = np.argwhere(sel).flatten()

        PMTRWFs_Array .append( pmtrwfs[idxs, :, :] )
        SIPMRWFs_Array.append( sipmwfs[idxs, :, :] )
        
        for ev, t in events_time[idxs]:
            EI["event"] = ev
            EI["time"]  = t
            EI.append()
    
    WF_file.close()
    
PMTRWFs_Array   .flush()
SIPMRWFs_Array  .flush()
Event_Info_table.flush()

h5file.close()
