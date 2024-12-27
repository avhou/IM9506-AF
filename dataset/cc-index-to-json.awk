BEGIN {
    FS = " ";
    OFS = " ";
}

{
    # Extract the first and second fields
    # ignore urlkey
    urlkey = $1;
    timestamp = $2;

    # Extract the JSON part (handles spaces in the JSON field)
    json = "";
    for (i = 3; i <= NF; i++) {
        json = json $i (i < NF ? OFS : "");
    }

    # Remove the trailing comma if it exists
    sub(/\}$/, ", \"timestamp\": \"" timestamp "\"}", json);

    # Print the updated line with the modified JSON
    print json;
}
