import glob
import numpy  as np
import tables as tb

## PARAMS
run = 6731
wf_dir     = f"/home/gdiaz/DEMO++/DATA/{run}/waveforms/"
event_data = f"/home/gdiaz/DEMO++/longitudinal/selected_events_{run}.npy"
out_dir    = f"/home/gdiaz/DEMO++/DATA/{run}/selected_waveforms/"


# WF FILES AND EVENTS
wf_files   = glob.glob(wf_dir  + "/*")
wf_files.sort()


## LOAD EVENTS
events = np.load( event_data )


def create_file(number):
	## CREATE FILE TO SAVE RWFS AND EVENTS
	h5file = tb.open_file(out_dir + "/" + f"run_{run}_{number}_selected.h5", mode="w", title="selected wfs")
	selrwfs_group    = h5file.create_group("/", "RD")
	event_info_group = h5file.create_group("/", "Run")
	
	PMTRWFs_Array  = h5file.create_earray(selrwfs_group, "pmtrwf",
                              	tb.Int16Atom(), shape=(0, 3, 32000))
	SIPMRWFs_Array = h5file.create_earray(selrwfs_group, "sipmrwf",
                              	tb.Int16Atom(), shape=(0, 256, 800))
	
	class Event_Info(tb.IsDescription):
		event = tb.Int32Col()
		time  = tb.UInt64Col()
	
	Event_Info_table = h5file.create_table(event_info_group, "events", Event_Info, "selected events")
	#EI = Event_Info_table.row

	return h5file, PMTRWFs_Array, SIPMRWFs_Array, Event_Info_table

## SELECT AND WRITE
for file in wf_files:
	number = file.split("/")[-1].split("_")[2]
	print(number, "/", len(wf_files) )

	#create file for each wf file
	h5file, PMTRWFs_Array, SIPMRWFs_Array, Event_Info_table = create_file(number)
	EI = Event_Info_table.row

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
