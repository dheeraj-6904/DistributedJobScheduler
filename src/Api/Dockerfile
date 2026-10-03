FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS base
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src
COPY ["src/Api/DistributedJobScheduler.Api.csproj", "src/Api/"]
COPY ["src/Core/DistributedJobScheduler.Core.csproj", "src/Core/"]
RUN dotnet restore "src/Api/DistributedJobScheduler.Api.csproj"
COPY src/ src/
WORKDIR "/src/src/Api"
RUN dotnet build "DistributedJobScheduler.Api.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "DistributedJobScheduler.Api.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "DistributedJobScheduler.Api.dll"]
