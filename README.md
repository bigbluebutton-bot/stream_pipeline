# Pipeline
```mermaid
stateDiagram-v2
    [*] --> ReceiveAudioStream: Start
    

    state audio_fork <<fork>>
    ReceiveAudioStream --> audio_fork
    audio_fork --> Buffer_of_n_seconds_audio: Process audio chunk
    audio_fork --> ReceiveAudioStream: Receve next audio chunk

    Buffer_of_n_seconds_audio --> Pipeline: Process audio buffer
    note left of Buffer_of_n_seconds_audio
        Creates a buffer of n seconds of audio data
    end note

    state Pipeline {
        [*] --> PackIntoPackages: Start Pipeline
        state parallelPipe <<fork>>
        gRPCreceve --> parallelPipe
        PackIntoPackages --> parallelPipe
        parallelPipe --> PreProcessing: 0
        parallelPipe --> PreProcessing: 1
        parallelPipe --> PreProcessing: (...n)

        state PreProcessing {

            [*] --> PreModule0: Start Pre-Processing

            state PreModule0 {
                [*] --> PreExecuteCode0
                PreExecuteCode0 --> [*]
            }

            PreModule0 --> PreModule1

            state PreModule1 {
                [*] --> PreExecuteCode1
                PreExecuteCode1 --> [*]
            }

            PreModule1 --> PreModule...n

            state PreModule...n {
                [*] --> PreExecuteCode...n
                PreExecuteCode...n --> [*]
            }

            PreModule...n --> [*]: End Pre-Processing
        }
        
        PreProcessing --> MainProcessing: 0
        PreProcessing --> MainProcessing: 1
        PreProcessing --> MainProcessing: (...n)
        
        state MainProcessing {
            [*] --> MainModule0: Start Main-Processing

            state MainModule0 {
                [*] --> MainExecuteCode0
                MainExecuteCode0 --> [*]
            }

            MainModule0 --> MainModule1

            state MainModule1 {
                [*] --> MainExecuteCode1
                MainExecuteCode1 --> [*]
            }

            MainModule1 --> MainModule...n

            state MainModule...n {
                [*] --> MainExecuteCode...n
                MainExecuteCode...n --> [*]
            }

            MainModule...n --> [*]: End Main-Processing
        }
        
        MainProcessing --> PostProcessing: 0
        MainProcessing --> PostProcessing: 1
        MainProcessing --> PostProcessing: (...n)
        
        state PostProcessing {
            [*] --> PostModule0: Start Post-Processing

            state PostModule0 {
                [*] --> PostExecuteCode0
                PostExecuteCode0 --> [*]
            }

            PostModule0 --> PostModule1

            state PostModule1 {
                [*] --> PostExecuteCode1
                PostExecuteCode1 --> [*]
            }

            PostModule1 --> PostModule...n

            state PostModule...n {
                [*] --> PostExecuteCode...n
                PostExecuteCode...n --> [*]
            }

            PostModule...n --> [*]: End Post-Processing
        }

        state parallelPipeJoin <<join>>
        PostProcessing --> parallelPipeJoin: 0
        PostProcessing --> parallelPipeJoin: 1
        PostProcessing --> parallelPipeJoin: (...n)

        state if_state <<choice>>
        parallelPipeJoin --> if_state: If receved over gRPC
        if_state --> gRPCreturn: True
        if_state --> OrderPackages: False

        OrderPackages --> [*]: End Pipeline
    }

    Pipeline --> DoStuff
    DoStuff --> [*]: End


```

# Modules
## ExecutionModule
```mermaid
stateDiagram-v2
    [*] --> ExecutionModule

    state ExecutionModule {
        [*] --> ExecuteCode
        ExecuteCode --> [*]
    }

    ExecutionModule --> [*]
```

## ConditionModule
```mermaid
stateDiagram-v2
    [*] --> ConditionModule


    state ConditionModule {
        state if_state <<choice>>
        [*] --> ConditionCode
        ConditionCode --> if_state
        if_state --> TrueModule: If condition is true
        if_state --> FalseModule : If condition is false

        TrueModule --> [*]
        FalseModule --> [*]
    }

    ConditionModule --> [*]
```

## CombinationModule
```mermaid
stateDiagram-v2
    [*] --> CombinationModule

    state CombinationModule {
        [*] --> Module0: Start Processing

        state Module0 {
            [*] --> ExecuteCode0
            ExecuteCode0 --> [*]
        }

        Module0 --> Module1

        state Module1 {
            [*] --> ExecuteCode1
            ExecuteCode1 --> [*]
        }

        Module1 --> Module...n

        state Module...n {
            [*] --> ExecuteCode...n
            ExecuteCode...n --> [*]
        }

        Module...n --> [*]
    }

    CombinationModule --> [*]
```

## ExternalPipelineModule
```mermaid
stateDiagram-v2
    state Server0 {
        state Pipeline0 {
            state ExternalPipelineModule {
                [*] --> StartExecuteCodeOnExternalPipeline1
                EndExecuteCodeOnExternalPipeline1 --> [*]
            }
        }
    }

    state Server1 {
        StartPipeline1 --> Pipeline1
        state Pipeline1 {
            [*] --> PreProcessing
            PreProcessing --> MainProcessing
            MainProcessing --> PostProcessing
            PostProcessing --> [*]
        }
        Pipeline1 --> EndPipeline1
    }

    StartExecuteCodeOnExternalPipeline1 --> StartPipeline1
    EndPipeline1 --> EndExecuteCodeOnExternalPipeline1
```