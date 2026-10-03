FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src
COPY ["src/Worker/DistributedJobScheduler.Worker.csproj", "src/Worker/"]
COPY ["src/Core/DistributedJobScheduler.Core.csproj", "src/Core/"]
RUN dotnet restore "src/Worker/DistributedJobScheduler.Worker.csproj"
COPY src/ src/
WORKDIR "/src/src/Worker"
RUN dotnet build "DistributedJobScheduler.Worker.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "DistributedJobScheduler.Worker.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "DistributedJobScheduler.Worker.dll"]
