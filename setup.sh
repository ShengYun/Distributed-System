if [ "$HOSTNAME" = csa2.bu.edu -o "$HOSTNAME" = csa3.bu.edu ]; then
	printf '%s\n' "On csa server, setting up GOROOT and bin"
	echo "Setting GOROOT"
	export GOROOT=/research/sesa/451/go
	echo "Setting Go Bin"
	export PATH=$PATH:$GOROOT/bin
fi

echo "Setting up GOPATH"
export GOPATH=$PWD
