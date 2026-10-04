#!/bin/bash
if ! command -v dotnet &> /dev/null
then
    echo "Installing .NET 9.0 SDK..."
    wget -qO- https://dot.net/v1/dotnet-install.sh | bash -s -- --channel 9.0
fi
export DOTNET_ROOT=$HOME/.dotnet
export PATH=$PATH:$HOME/.dotnet:$HOME/.dotnet/tools
cd /mnt/c/Users/dheer/Desktop/DistributedJobScheduler
export DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1
dotnet test tests/IntegrationTests/DistributedJobScheduler.IntegrationTests.csproj --filter FullFlow_ApiToSchedulerToWorker --logger "console;verbosity=detailed"
