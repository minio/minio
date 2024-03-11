#! /usr/bin/env bash
dir="$( cd "$( dirname $0 )" && pwd )"
reldir=${dir##$PWD}
if [[ "$dir" =~ ^"$PWD" ]]; then
    reldir=".$reldir"
fi
cd $dir

set -eu
if ! [[ "$(python --version)" =~ Python" "3 ]]; then
    echo "'python' command must be python 3!"
    exit 1
fi
python -m venv env
source $dir/env/bin/activate
pip install -r $dir/requirements.txt

if ! [ -f "$dir/.env" ]; then
    cp "$dir/.env.example" "$dir/.env"
fi

echo "
In vscode you can use the "Python: Select Interpreter" command

To activate the python venv run the following:
source \"$reldir/env/bin/activate\"

To run the service in development:
\"$reldir/web_identity.py\"

"

