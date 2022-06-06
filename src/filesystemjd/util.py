def create_file( filename, contents = "new file" ):
    with open( filename, 'w' ) as f:
        f.write( contents )
