
# GoLANG test5 BACKEND

This project is a backend app to read event.json file [collection of UUID key/value pairs], writes them into a db.bin file 
and can then search the db.bin file for present of a user provided UUIDs.

## Usage

`./main -h` Help menu
`./main user-provided-uuid-#1 user-provided-uuid-#2 [Space separated list of UUID to search]` Search user provided UUIDs
`./main -g user-provided-uuid-#1 user-provided-uuid-#2 [Weather to generate db.bin]` Re-generate db.bin and then search it for user provided UUIDs

    
## Build

Run `make build` to build the project. Main executable will be generated in the project root. 


