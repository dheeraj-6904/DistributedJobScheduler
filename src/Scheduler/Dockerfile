FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src
COPY ["src/Scheduler/DistributedJobScheduler.Scheduler.csproj", "src/Scheduler/"]
COPY ["src/Core/DistributedJobScheduler.Core.csproj", "src/Core/"]
RUN dotnet restore "src/Scheduler/DistributedJobScheduler.Scheduler.csproj"
COPY src/ src/
WORKDIR "/src/src/Scheduler"
RUN dotnet build "DistributedJobScheduler.Scheduler.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "DistributedJobScheduler.Scheduler.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "DistributedJobScheduler.Scheduler.dll"]
